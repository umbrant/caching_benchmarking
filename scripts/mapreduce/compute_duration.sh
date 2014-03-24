#!/bin/bash

if [ "$#" -ne 1 ]; then echo "$0 <output folder from MR job>"; exit -1; fi

if [ ! -d $1 ]; then
    echo "Expected path to be a folder"
    exit -1
fi

MR=$1
shift

for f in `ls $MR`; do
    FILE=`pwd`/$MR/$f
    START=`grep "map 0" $FILE | cut -d " " -f 2 | head -1`
    MAP_END=`grep "map 100" $FILE | cut -d " " -f 2 | head -1`
    END=`grep "reduce 100" $FILE | cut -d " " -f 2 | head -1`

    START_TIMESTAMP=`date --date="$START" +%s`
    MAP_END_TIMESTAMP=`date --date="$MAP_END" +%s`
    END_TIMESTAMP=`date --date="$END" +%s`

    echo -n "Map:" $(( $MAP_END_TIMESTAMP - $START_TIMESTAMP ))
    echo -n ", Reduce:" $(( $END_TIMESTAMP - $MAP_END_TIMESTAMP ))
    echo -n ", Total:" $(( $END_TIMESTAMP - $START_TIMESTAMP ))
    echo
done
