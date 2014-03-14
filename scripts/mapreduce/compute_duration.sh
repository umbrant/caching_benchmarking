#!/bin/bash

if [ "$#" -ne 1 ]; then echo "$0 <output from MR job>"; exit -1; fi

MR=$1
shift

START=`grep "map 0" $MR | cut -d " " -f 2`
END=`grep "reduce 100" $MR | cut -d " " -f 2`

START_TIMESTAMP=`date --date="$START" +%s`
END_TIMESTAMP=`date --date="$END" +%s`

echo "Seconds:" $(( $END_TIMESTAMP - $START_TIMESTAMP ))
