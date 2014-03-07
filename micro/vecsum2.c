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

#include "immintrin.h"

#define VECSUM_CHUNK_SIZE (8 * 1024 * 1024)
#define NORMAL_READ_CHUNK_SIZE (8 * 1024)
#define DOUBLES_PER_LOOP_ITER 8

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

static int check_byte_size(int byte_size, const char *const str)
{
	if (byte_size % sizeof(double)) {
		fprintf(stderr, "%s is not a multiple "
			"of sizeof(double)\n", str);
		return EINVAL;
	}
	if ((byte_size / sizeof(double)) % DOUBLES_PER_LOOP_ITER) {
		fprintf(stderr, "The number of doubles contained in "
			"%s is not a multiple of DOUBLES_PER_LOOP_ITER\n",
			str);
		return EINVAL;
	}
	return 0;
}

#ifdef SIMPLE_VECSUM

static double vecsum(const double *buf, int num_doubles)
{
	int i;
	double sum = 0.0;
	for (i = 0; i < num_doubles; i++) {
		sum += buf[i];
	}
	return sum;
}

#else

static double vecsum(const double *buf, int num_doubles)
{
	int i;
	double hi, lo;
	__m128d x0 = _mm_set_pd(0.0,0.0);
	__m128d x1 = _mm_set_pd(0.0,0.0);
	__m128d x2 = _mm_set_pd(0.0,0.0);
	__m128d x3 = _mm_set_pd(0.0,0.0);
	__m128d sum0 = _mm_set_pd(0.0,0.0);
	__m128d sum1 = _mm_set_pd(0.0,0.0);
	__m128d sum2 = _mm_set_pd(0.0,0.0);
	__m128d sum3 = _mm_set_pd(0.0,0.0);
	for (i = 0; i < num_doubles; i+=DOUBLES_PER_LOOP_ITER) {
		x0 = _mm_load_pd(buf + i + 0);
		sum0 = _mm_add_pd(sum0, x0);
		x1 = _mm_load_pd(buf + i + 0);
		sum1 = _mm_add_pd(sum1, x1);
		x2 = _mm_load_pd(buf + i + 0);
		sum2 = _mm_add_pd(sum2, x2);
		x3 = _mm_load_pd(buf + i + 0);
		sum3 = _mm_add_pd(sum3, x3);
	}
	x0 = _mm_set_pd(0.0,0.0);
	x0 = _mm_add_pd(x0, sum0);
	x0 = _mm_add_pd(x0, sum1);
	x0 = _mm_add_pd(x0, sum2);
	x0 = _mm_add_pd(x0, sum3);
	_mm_storeh_pd(&hi, x0);
	_mm_storel_pd(&lo, x0);
	return hi + lo;
}

#endif

static int vecsum_zcr_loop(int pass, struct test_data *tdata,
		struct hadoopRzOptions *zopts, const struct options *opts)
{
	int32_t len;
	double sum = 0.0;
	const double *buf;
	struct hadoopRzBuffer *rzbuf = NULL;
	int ret;

	while (1) {
		rzbuf = hadoopReadZero(tdata->file, zopts, VECSUM_CHUNK_SIZE);
		if (!rzbuf) {
			ret = errno;
			fprintf(stderr, "hadoopReadZero failed with error "
				"code %d (%s)\n", ret, strerror(ret));
			goto done;
		}
		len = hadoopRzBufferLength(rzbuf);
		if (len == 0) break;
		if (len < VECSUM_CHUNK_SIZE) {
			fprintf(stderr, "hadoopReadZero got a partial read "
				"of length %d\n", len);
			ret = EINVAL;
			goto done;
		}
		buf = hadoopRzBufferGet(rzbuf);
		sum += vecsum(buf, VECSUM_CHUNK_SIZE / sizeof(double));
		hadoopRzBufferFree(tdata->file, rzbuf);
	}
	printf("finished zcr pass %d.  sum = %g\n", pass, sum);
	ret = 0;

done:
	if (rzbuf)
		hadoopRzBufferFree(tdata->file, rzbuf);
	return ret;
}

static int vecsum_zcr(struct test_data *tdata, const struct options *opts)
{
	int ret, pass;
	struct hadoopRzOptions *zopts = NULL;

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
	for (pass = 0; pass < opts->passes; ++pass) {
		ret = vecsum_zcr_loop(pass, tdata, zopts, opts);
		if (ret) {
			fprintf(stderr, "vecsum_zcr_loop pass %d failed "
				"with error %d\n", pass, ret);
			goto done;
		}
		hdfsSeek(tdata->fs, tdata->file, 0);
	}
	ret = 0;
done:
	if (zopts)
		hadoopRzOptionsFree(zopts);
	return ret;
}

static int vecsum_normal_loop(int pass, struct test_data *tdata,
			const struct options *opts)
{
	double sum = 0.0;

	while (1) {
		int res = hdfsRead(tdata->fs, tdata->file, tdata->buf,
				NORMAL_READ_CHUNK_SIZE);
		if (res == 0) // EOF
			break;
		if (res < 0) {
			int err = errno;
			fprintf(stderr, "hdfsRead failed with error %d (%s)\n",
				err, strerror(err));
			return err;
		}
		if (res < NORMAL_READ_CHUNK_SIZE) {
			fprintf(stderr, "hdfsRead got a partial read of "
				"length %d\n", res);
			return EINVAL;
		}
		sum += vecsum(tdata->buf, NORMAL_READ_CHUNK_SIZE / sizeof(double));
	}
	printf("finished normal pass %d.  sum = %g\n", pass, sum);
	return 0;
}

static int vecsum_normal(struct test_data *tdata, const struct options *opts)
{
	int pass;

	tdata->buf = malloc(NORMAL_READ_CHUNK_SIZE);
	if (!tdata->buf) {
		fprintf(stderr, "failed to malloc buffer of size %d\n",
			NORMAL_READ_CHUNK_SIZE);
		return ENOMEM;
	}
	for (pass = 0; pass < opts->passes; ++pass) {
		int ret = vecsum_normal_loop(pass, tdata, opts);
		if (ret) {
			fprintf(stderr, "vecsum_normal_loop pass %d failed "
				"with error %d\n", pass, ret);
			return ret;
		}
		hdfsSeek(tdata->fs, tdata->file, 0);
	}
	return 0;
}

int main(void)
{
	int ret = 1;
	struct options *opts = NULL;
	struct test_data *tdata = NULL;

	if (check_byte_size(VECSUM_CHUNK_SIZE, "VECSUM_CHUNK_SIZE") ||
		check_byte_size(NORMAL_READ_CHUNK_SIZE,
				"NORMAL_READ_CHUNK_SIZE")) {
		goto done;
	}
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
