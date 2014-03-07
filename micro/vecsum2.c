/*
 * vim: ts=8:sw=8:tw=79:noet
 */
#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <hdfs.h>
#include <malloc.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "immintrin.h"

#define VECSUM_CHUNK_SIZE (8 * 1024 * 1024)
#define ZCR_READ_CHUNK_SIZE (1024 * 1024 * 8) // 8
#define NORMAL_READ_CHUNK_SIZE (4 * 1024 * 1024)
#define DOUBLES_PER_LOOP_ITER 8

static double timespec_to_double(struct timespec *ts)
{
	double sec = ts->tv_sec;
	double nsec = ts->tv_nsec;
	return sec + (nsec / 1000000000L);
}

struct stopwatch {
	struct timespec start;
	struct timespec stop;
	struct rusage rusage;
};

static struct stopwatch *stopwatch_create(void)
{
	struct stopwatch *watch;

	watch = calloc(1, sizeof(struct stopwatch));
	if (!watch) {
		fprintf(stderr, "failed to allocate memory for stopwatch\n");
		goto error;
	}
	if (clock_gettime(CLOCK_MONOTONIC, &watch->start)) {
		int err = errno;
		fprintf(stderr, "clock_gettime(CLOCK_MONOTONIC) failed with "
			"error %d (%s)\n", err, strerror(err));
		goto error;
	}
	if (getrusage(RUSAGE_THREAD, &watch->rusage) < 0) {
		int err = errno;
		fprintf(stderr, "getrusage failed: error %d (%s)\n",
			err, strerror(err));
		goto error;
	}
	return watch;

error:
	free(watch);
	return NULL;
}

static void stopwatch_stop(struct stopwatch *watch, long long bytes_read)
{
	double elapsed, rate;

	if (clock_gettime(CLOCK_MONOTONIC, &watch->stop)) {
		int err = errno;
		fprintf(stderr, "clock_gettime(CLOCK_MONOTONIC) failed with "
			"error %d (%s)\n", err, strerror(err));
		goto done;
	}
	elapsed = timespec_to_double(&watch->stop) -
		timespec_to_double(&watch->start);
	rate = (bytes_read / elapsed) / (1024 * 1024 * 1024);
	printf("stopwatch: took %.4g seconds to read %lld bytes, "
		"for %.4g GB/s\n", elapsed, bytes_read, rate);
	printf("stopwatch:  %.4g seconds\n", elapsed);
done:
	free(watch);
}

struct options {
	// The path to read.
	const char *path;

	// The number of times to read the path.
	int passes;

	// Nonzero if we should use ZCR
	int zcr;

	// Nonzero if we should skip the summation.
	int skip_sum;
};

static struct options *options_create(void)
{
	struct options *opts = NULL;
	const char *pass_str;
	const char *zcr_str;
	const char *skip_sum_str;

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
	skip_sum_str = getenv("VECSUM_SKIP_SUM");
	if (!skip_sum_str) {
		fprintf(stderr, "You must set the VECSUM_SKIP_SUM environment "
			"variable to either 0 or 1 to disable or "
			"enable vector sums.\n");
		goto error;
	}
	opts->skip_sum = !!atoi(skip_sum_str);
	zcr_str = getenv("VECSUM_ZCR");
	if (!zcr_str) {
		fprintf(stderr, "You must set the VECSUM_ZCR environment "
			"variable to either 0 or 1 to disable or "
			"enable zcr.\n");
		goto error;
	}
	opts->zcr = !!atoi(zcr_str);
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
	long long length;
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
	tdata->length = pinfo->mSize;
	if (tdata->length == 0) {
		fprintf(stderr, "file %s has size 0.\n", opts->path);
		goto error;
	}
	if (tdata->length % VECSUM_CHUNK_SIZE) {
		fprintf(stderr, "file %s has size %lld, which is not aligned with "
			"our VECSUM_CHUNK_SIZE of %d\n",
			opts->path, tdata->length, VECSUM_CHUNK_SIZE);
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

static double vecsum(const struct options *opts,
		const double *buf, int num_doubles)
{
	int i;
	double hi, lo;
	if (opts->skip_sum)
		return 0.0;
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
		x1 = _mm_load_pd(buf + i + 2);
		x2 = _mm_load_pd(buf + i + 4);
		x3 = _mm_load_pd(buf + i + 6);
		sum0 = _mm_add_pd(sum0, x0);
		sum1 = _mm_add_pd(sum1, x1);
		sum2 = _mm_add_pd(sum2, x2);
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
		rzbuf = hadoopReadZero(tdata->file, zopts, ZCR_READ_CHUNK_SIZE);
		if (!rzbuf) {
			ret = errno;
			fprintf(stderr, "hadoopReadZero failed with error "
				"code %d (%s)\n", ret, strerror(ret));
			goto done;
		}
		len = hadoopRzBufferLength(rzbuf);
		buf = hadoopRzBufferGet(rzbuf);
		if (!buf) break;
		if (len < ZCR_READ_CHUNK_SIZE) {
			fprintf(stderr, "hadoopReadZero got a partial read "
				"of length %d\n", len);
			ret = EINVAL;
			goto done;
		}
		sum += vecsum(opts, buf,
			ZCR_READ_CHUNK_SIZE / sizeof(double));
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
		sum += vecsum(opts, tdata->buf,
			      NORMAL_READ_CHUNK_SIZE / sizeof(double));
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
	struct stopwatch *watch = NULL;

	if (check_byte_size(VECSUM_CHUNK_SIZE, "VECSUM_CHUNK_SIZE") ||
		check_byte_size(ZCR_READ_CHUNK_SIZE,
				"ZCR_READ_CHUNK_SIZE") ||
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
	watch = stopwatch_create();
	if (!watch)
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
	if (watch)
		stopwatch_stop(watch, tdata->length * opts->passes);
	if (tdata)
		test_data_free(tdata);
	if (opts)
		options_free(opts);
	return ret;
}
