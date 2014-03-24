#!/bin/bash
set -e

if [ "$#" -ne 3 ]; then echo "$0 <grep/wc/bc/bc2g> <number of runs> <output folder>"; exit -1; fi

# Useful constants

HIN="/wikipedia_snapshots/dewiki-20121117-pages-meta-current.xml"
HIN_2G="/wikipedia_snapshots/dewiki-20121117-pages-meta-current.xml.2GB"

EXAMPLE_JAR="/home/cloudera/parcels/CDH-5.0.0-1.cdh5.0.0.p0.36/lib/hadoop-0.20-mapreduce/hadoop-examples-2.3.0-mr1-cdh5.0.0-SNAPSHOT.jar"
BYTECOUNT_JAR="/home/cmccabe/2014_caching_benchmarks/mapreduce/ByteCount/target/bytecount-1.0-SNAPSHOT.jar"

# Command specification

JOB=
JAR=
# Used for grep job
PATTERN=
OPTS=

# Init parameters

if [ "$1" = "grep" ]; then
	JOB="grep"
	JAR=$EXAMPLE_JAR
	PATTERN="qwertyuiopasdfghjklzxcvbnm"
elif [ "$1" = "wc" ]; then
	JOB="wordcount"
	JAR=$EXAMPLE_JAR
elif [ "$1" = "bc" ]; then
	JOB="com.cloudera.ByteCount"
	JAR=$BYTECOUNT_JAR
elif [ "$1" = "bc2g" ]; then
	JOB="com.cloudera.ByteCount"
	OPTS="-skipChecksums -D mapreduce.child.java.opts=-Xmx5120 -D mapreduce.map.memory.mb=5120"
	JAR=$BYTECOUNT_JAR
	HIN=$HIN_2G
else
	echo "Unknown job $1"
fi
shift
RUNS=$1
shift
OUTFOLDER=$1
shift

mkdir -p $OUTFOLDER

echo "Running $JOB $RUNS times, saving output to $OUTFOLDER"


for x in `seq 1 $RUNS`; do
	echo "Doing run $x..."
	HOUT="/temp-${x}.out"
	../drop_caches.sh
	hadoop fs -rm -r -skipTrash $HOUT || true
	hadoop jar $JAR $JOB $OPTS $HIN $HOUT $PATTERN &> $OUTFOLDER/$JOB-$x.out
done

echo "Done!"
