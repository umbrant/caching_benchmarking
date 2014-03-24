#!/bin/bash

if [ "$#" -ne 3 ]; then echo "$0 <grep/wc> <number of runs> <output folder>"; exit -1; fi

# Useful constants
HIN="/wikipedia_snapshots/dewiki-20121117-pages-meta-current.xml"
EXAMPLE_JAR="/home/cloudera/parcels/CDH-5.0.0-1.cdh5.0.0.p0.36/lib/hadoop-0.20-mapreduce/hadoop-examples-2.3.0-mr1-cdh5.0.0-SNAPSHOT.jar"

# Command specification

JOB=
JAR=
# Used for grep job
PATTERN=

# Init parameters

if [ "$1" = "grep" ]; then
	JOB="grep"
	JAR=$EXAMPLE_JAR
	PATTERN="qwertyuiopasdfghjklzxcvbnm"
elif [ "$1" = "wc" ]; then
	JOB="wordcount"
	JAR=$EXAMPLE_JAR
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
	hadoop fs -rm -r -skipTrash $HOUT
	hadoop jar $JAR $JOB $HIN $HOUT $PATTERN &> $OUTFOLDER/$JOB-$x.out
done

echo "Done!"
