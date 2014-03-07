#include "hdfs.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <malloc.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <string.h>
#include "immintrin.h"

// 1GB
static const int size = 1024 * 1024 * 1024;

long get_ns(struct timespec *t) {
  return (t->tv_sec * 1000 * 1000 * 1000) + t->tv_nsec;
}

void print_duration(struct timespec *start, struct timespec *end, long size) {
  long diff = get_ns(end) - get_ns(start);
  double tput = ((double)size / (double)(1000*1000)) / ( (double)diff / (double)(1000*1000*1000));
  printf("Took %ld ns\n", diff);
  printf("Throughput (MB/s): %f\n", tput);
}

void gettime(struct timespec *t) {
  clock_gettime(CLOCK_PROCESS_CPUTIME_ID, t);
}

void usage() {
    printf("Usage: <1GB+ file> <number of iterations> <method [mrzh]>\n");
    printf("m - local memory read\n");
    printf("r - local normal read\n");
    printf("z - hdfs zero copy read\n");
    printf("h - hdfs normal read\n");
    exit(-1);
}

int main(int argc, char*argv[]) {
  struct hadoopRzOptions *zopts = NULL;
  struct hadoopRzBuffer *rzbuf = NULL;
  if (argc < 4) {
    usage();
  }
  char* filename = argv[1];
  int num_iters = atoi(argv[2]);
  char method = *argv[3];
  if (NULL == strchr("mrzh", method)) {
    usage();
  }

  int ret;

  void* aligned = NULL;
  // If local mem, copy file into a local mlock'd aligned buffer
  if (method == 'm') {
    printf("Creating %d of aligned data...\n", size);
    aligned = memalign(32, size);
    if (aligned == NULL) {
      perror("memalign");
      exit(3);
    }
    // Read the specified file in buffer
    int fd = open(filename, O_RDONLY);
    int total_bytes = 0;
    while (total_bytes < size) {
      int bytes = read(fd, aligned+total_bytes, size-total_bytes);
      if (bytes == -1) {
        perror("read");
        exit(-1);
      }
      total_bytes += bytes;
    }

    printf("Attempting mlock of buffer\n");
    ret = mlock(aligned, size);
    if (ret != 0) {
      perror("mlock");
      exit(2);
    }
  }

  printf("Summing output %d times...\n", num_iters);
  int i, j, k, l;
  // Copy data into this intermediate buffer
  const int buffer_size = (8*1024*1024);
  void *temp_buffer;
  ret = posix_memalign(&temp_buffer, 32, buffer_size);
  if (ret != 0) {
    printf("error in posix_memalign\n");
    exit(ret);
  }
  // This is for loop unrolling (unroll 4 times)
  __m128d* tempd = memalign(32, 16*4);
  struct timespec start, end;
  if (tempd == NULL) {
    perror("memalign");
    exit(3);
  }
  const int print_iters = 10;
  double end_sum = 0;

  hdfsFS fs = NULL;
  if (method == 'h' || method == 'z') {
    struct hdfsBuilder *builder = hdfsNewBuilder();

    hdfsBuilderSetNameNode(builder, "default");
    hdfsBuilderConfSetStr(builder, "dfs.client.read.shortcircuit.skip.checksum",
                          "true");
    fs = hdfsBuilderConnect(builder);
    if (fs == NULL) {
      printf("Could not connect to default namenode!\n");
      exit(-1);
    }
  }

  for (i=0; i<num_iters; i+=print_iters) {
    gettime(&start);
    __m128d sum;
    // Number of packed doubles we've processed
    for (j=0; j<print_iters; j++) {
      int offset = 0;
      int fd = 0;
      hdfsFile hdfsFile = NULL;

      if (method == 'r') {
        fd = open(filename, O_RDONLY);
      }
      // hdfs zerocopy read
      else if (method == 'z') {
        zopts = hadoopRzOptionsAlloc();
        if (!zopts) abort();
        if (hadoopRzOptionsSetSkipChecksum(zopts, 1)) abort();
        if (hadoopRzOptionsSetByteBufferPool(zopts, NULL)) abort();
        hdfsFile = hdfsOpenFile(fs, filename, O_RDONLY, 0, 0, 0);
      }
      // hdfs normal read
      else if (method == 'h') {
        hdfsFile = hdfsOpenFile(fs, filename, O_RDONLY, 0, 0, 0);
      }

      // Each iteration, process the buffer once
      for (k=0; k<size; k+=buffer_size) {
        // Set this with varying methods!
        const double* buffer = NULL;

        // Local file read
        if (method == 'r') {
          // do read
          int total_bytes = 0;
          while (total_bytes < buffer_size) {
            int bytes = read(fd, temp_buffer+total_bytes, buffer_size-total_bytes);
            if (bytes < 0) {
              printf("Error on read\n");
              return -1;
            }
            total_bytes += bytes;
          }
          buffer = (double*)temp_buffer;
        }
        // Local memory read
        else if (method == 'm') {
          buffer = (double*)(aligned + offset);
        }
        // hdfs zerocopy read
        else if (method == 'z') {
          int len;
          rzbuf = hadoopReadZero(hdfsFile, zopts, buffer_size);
          if (!rzbuf) abort();
          buffer = hadoopRzBufferGet(rzbuf);
          if (!buffer) abort();
          len = hadoopRzBufferLength(rzbuf);
          if (len < buffer_size) abort();
        }
        // hdfs normal read
        else if (method == 'h') {
          abort(); // need to implement hdfsReadFully
          //ret = hdfsReadFully(fs, hdfsFile, temp_buffer, buffer_size);
          if (ret == -1) {
            printf("Error: hdfsReadFully errored\n");
            exit(-1);
          }
          buffer = temp_buffer;
        }

        offset += buffer_size;

        // Unroll the loop a bit
        const double* a_ptr = &(buffer[0]);
        const double* b_ptr = &(buffer[2]);
        const double* c_ptr = &(buffer[4]);
        const double* d_ptr = &(buffer[6]);
        for (l=0; l<buffer_size; l+=64) {
          tempd[0] = _mm_load_pd(a_ptr);
          tempd[1] = _mm_load_pd(b_ptr);
          tempd[2] = _mm_load_pd(c_ptr);
          tempd[3] = _mm_load_pd(d_ptr);
          sum = _mm_add_pd(sum, tempd[0]);
          sum = _mm_add_pd(sum, tempd[1]);
          sum = _mm_add_pd(sum, tempd[2]);
          sum = _mm_add_pd(sum, tempd[3]);
          a_ptr += 8;
          b_ptr += 8;
          c_ptr += 8;
          d_ptr += 8;
        }
      }
      // Local file read
      if (method == 'r') {
        close(fd);
      }
      // hdfs zerocopy read
      // hdfs normal read
      else if (method == 'z' || method == 'h') {
        if (method == 'z') {
          hadoopRzBufferFree(hdfsFile, rzbuf);
        }
        hdfsCloseFile(fs, hdfsFile);
      }
      printf("iter %d complete\n", j);
    }
    gettime(&end);
    print_duration(&start, &end, (long)size*print_iters);
    // Force the compiler to actually generate above code
    double* unpack = (double*)&sum;
    double final = unpack[0] + unpack[1];
    end_sum += final;
  }
  if (method == 'z' || method == 'h') {
    hdfsDisconnect(fs);
  }
  printf("%f\n", end_sum);
  return 0;
}
