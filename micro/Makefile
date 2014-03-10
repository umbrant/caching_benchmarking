
# Note: you must set HADOOP_HOME_BASE to the root directory where
# a Hadoop tarball is installed.

CFLAGS=-Wall -Werror -mmmx -msse -msse2 -O3 -I$(HADOOP_HOME_BASE)/include -g -rdynamic
LDFLAGS=-lhdfs -lrt -L$(HADOOP_HOME_BASE)/lib/native

all: create-float-file vecsum1 vecsum2

create-float-file: create-float-file.o

vecsum1: vecsum1.o

vecsum2: vecsum2.o

clean:
	rm -f create-float-file vecsum1 vecsum2 *.o
