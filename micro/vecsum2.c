/*
 * vim: ts=8:sw=8:tw=79:noet
 */
#include <errno.h>
#include <fcntl.h>
#include <hdfs.h>
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define VECSUM_CHUNK_SIZE (8 * 1024 * 1024)
#define NORMAL_READ_CHUNK_SIZE (8 * 1024)

struct options {
	// The path to read.
	const char *path;

	// The number of times to read the path.
	int passes;

	// Nonzero if we should use ZCR
	int zcr;
};

static struct options *options_create(void)
{
	struct options *opts = NULL;
	const char *pass_str;

	opts = calloc(1, sizeof(struct options));
	if (!opts) {
		fprintf(stderr, "failed to calloc options\n");
		goto error;
	}
	opts->path = getenv("VECSUM_PATH");
	if (!opts->path) {
		fprintf(stderr, "You must set the VECSUM_PATH environment "
			"variable to the path of the file to read.\n");
		goto error;
	}
	pass_str = getenv("VECSUM_PASSES");
	if (!pass_str) {
		fprintf(stderr, "You must set the VECSUM_PASSES environment "
			"variable to the number of passes to make.\n");
		goto error;
	}
	opts->passes = atoi(pass_str);
	if (opts->passes <= 0) {
		fprintf(stderr, "Invalid value for the VECSUM_PASSES "
			"environment variable.  You must set this to a "
			"number greater than 0.\n");
		goto error;
	}
	opts->zcr = !!getenv("VECSUM_ZCR");
	return opts;
error:
	free(opts);
	return NULL;
}

static void options_free(struct options *opts)
{
	free(opts);
}

struct test_data {
	hdfsFS fs;
	hdfsFile file;
	double *buf;
};

static void test_data_free(struct test_data *tdata)
{
	if (tdata->fs) {
		free(tdata->buf);
		if (tdata->file) {
			hdfsCloseFile(tdata->fs, tdata->file);
		}
		hdfsDisconnect(tdata->fs);
	}
}

static struct test_data *test_data_create(const struct options *opts)
{
	struct test_data *tdata = NULL;
	struct hdfsBuilder *builder = NULL;
	hdfsFileInfo *pinfo = NULL;
	
	tdata = calloc(1, sizeof(struct test_data));
	if (!tdata) {
		fprintf(stderr, "Failed to allocate test data.\n");
		goto error;
	}
	builder = hdfsNewBuilder();
	if (!builder) {
		fprintf(stderr, "Failed to create builder.\n");
		goto error;
	}
	hdfsBuilderSetNameNode(builder, "default");
	hdfsBuilderConfSetStr(builder, 
		"dfs.client.read.shortcircuit.skip.checksum", "true");
	tdata->fs = hdfsBuilderConnect(builder);
	builder = NULL;
	if (!tdata->fs) {
		fprintf(stderr, "Could not connect to default namenode!\n");
		goto error;
	}
	pinfo = hdfsGetPathInfo(tdata->fs, opts->path);
	if (!pinfo) {
		int err = errno;
		fprintf(stderr, "hdfsGetPathInfo(%s) failed: error %d (%s)\n",
			opts->path, err, strerror(err));
		goto error;
	}
	if (pinfo->mSize == 0) {
		fprintf(stderr, "file %s has size 0.\n", opts->path);
		goto error;
	}
	if (pinfo->mSize % VECSUM_CHUNK_SIZE) {
		fprintf(stderr, "file %s has size %lld, which is not aligned with "
			"our VECSUM_CHUNK_SIZE of %d\n",
			opts->path, (long long)pinfo->mSize, VECSUM_CHUNK_SIZE);
		goto error;
	}
	tdata->file = hdfsOpenFile(tdata->fs, opts->path, O_RDONLY, 0, 0, 0);
	if (!tdata->file) {
		int err = errno;
		fprintf(stderr, "hdfsOpenFile(%s) failed: error %d (%s)\n",
			opts->path, err, strerror(err));
		goto error;
	}
	return tdata;

error:
	if (pinfo)
		hdfsFreeFileInfo(pinfo, 1);
	if (tdata)
		test_data_free(tdata);
	if (builder)
		hdfsFreeBuilder(builder);
	return NULL;
}

static double vecsum(const double *buf, int byte_size)
{
	int i, size;
	double sum = 0.0;

	size = byte_size / sizeof(double);
	for (i = 0; i < size; i++) {
		sum += buf[i];
	}
	return sum;
}

static int vecsum_zcr(struct test_data *tdata, const struct options *opts)
{
	int ret;
	int32_t len;
	struct hadoopRzOptions *zopts = NULL;
	struct hadoopRzBuffer *rzbuf = NULL;
	double sum = 0.0;
	const double *buf;

	zopts = hadoopRzOptionsAlloc();
	if (!zopts) {
		fprintf(stderr, "hadoopRzOptionsAlloc failed.\n");
		ret = ENOMEM;
		goto done;
	}
	if (hadoopRzOptionsSetSkipChecksum(zopts, 1)) {
		ret = errno;
		perror("hadoopRzOptionsSetSkipChecksum failed: ");
		goto done;
	}
	if (hadoopRzOptionsSetByteBufferPool(zopts, NULL)) {
		ret = errno;
		perror("hadoopRzOptionsSetByteBufferPool failed: ");
		goto done;
	}
	while (1) {
		rzbuf = hadoopReadZero(tdata->file, zopts, VECSUM_CHUNK_SIZE);
		if (!rzbuf) {
			int err = errno;
			fprintf(stderr, "hadoopReadZero failed with error "
				"code %d (%s)\n", err, strerror(err));
			goto done;
		}
		len = hadoopRzBufferLength(rzbuf);
		if (len == 0) break;
		if (len < VECSUM_CHUNK_SIZE) {
			fprintf(stderr, "hadoopReadZero got a partial read "
				"of length %d\n", len);
			goto done;
		}
		buf = hadoopRzBufferGet(rzbuf);
		sum += vecsum(buf, VECSUM_CHUNK_SIZE);
		hadoopRzBufferFree(tdata->file, rzbuf);
	}
	printf("finished zcr pass.  sum = %g\n", sum);

	ret = 0;
done:
	if (rzbuf)
		hadoopRzBufferFree(tdata->file, rzbuf);
	if (zopts)
		hadoopRzOptionsFree(zopts);
	return ret;
}

static int vecsum_normal(struct test_data *tdata, const struct options *opts)
{
	int res;
	int ret = 1;
	double sum = 0.0;

	tdata->buf = malloc(NORMAL_READ_CHUNK_SIZE);
	if (!tdata->buf) {
		fprintf(stderr, "failed to malloc buffer of size %d\n",
			NORMAL_READ_CHUNK_SIZE);
		goto done;
	}
	while (1) {
		res = hdfsRead(tdata->fs, tdata->file, tdata->buf,
				NORMAL_READ_CHUNK_SIZE);
		if (res == 0) // EOF
			break;
		if (res < 0) {
			int err = errno;
			fprintf(stderr, "hdfsRead failed with error %d (%s)\n",
				err, strerror(err));
			goto done;
		}
		if (res < NORMAL_READ_CHUNK_SIZE) {
			fprintf(stderr, "hdfsRead got a partial read of "
				"length %d\n", res);
			goto done;
		}
		sum += vecsum(tdata->buf, NORMAL_READ_CHUNK_SIZE);
	}
	printf("finished normal pass.  sum = %g\n", sum);
	ret = 0;

done:
	return ret;
}

int main(void)
{
	int ret = 1;
	struct options *opts = NULL;
	struct test_data *tdata = NULL;

	opts = options_create();
	if (!opts)
		goto done;
	tdata = test_data_create(opts);
	if (!tdata)
		goto done;
	if (opts->zcr)
		ret = vecsum_zcr(tdata, opts);
	else
		ret = vecsum_normal(tdata, opts);
	if (ret) {
		fprintf(stderr, "vecsum failed with error %d\n", ret);
		goto done;
	}
	ret = 0;
done:
	if (tdata)
		test_data_free(tdata);
	if (opts)
		options_free(opts);
	return ret;
}
