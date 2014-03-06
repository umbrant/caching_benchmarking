
# Note: you must set HADOOP_HOME_BASE to the root directory where
# a Hadoop tarball is installed.

CFLAGS=-Wall -Werror -O3 -I$(HADOOP_HOME_BASE)/include
LDFLAGS=-lhdfs -L$(HADOOP_HOME_BASE)/lib/native

all: create-float-file vecsum2

create-float-file: create-float-file.o

vecsum2: vecsum2.o

clean:
	rm -f create-float-file vecsum2 *.o
