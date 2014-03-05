CFLAGS=-Wall -Werror -O2 -I$(HADOOP_HOME_BASE)/include
LDFLAGS=-lhdfs -L$(HADOOP_HOME_BASE)/lib/native

all: create-float-file vecsum2

create-float-file: create-float-file.o

vecsum2: vecsum2.o

clean:
	rm -f create-float-file vecsum2 *.o
