#!/bin/bash

set -e

OUTPUT="/tmp/wait-cache-$$"

while true; do
	hdfs cacheadmin -listDirectives -stats > $OUTPUT
	MET="true"
	IFS='
'
	for line in `cat $OUTPUT | tail -n +3`; do
		MYPATH=`echo $line | awk '{print $5}'`
		NEEDED=`echo $line | awk '{print $6}'`
		CACHED=`echo $line | awk '{print $7}'`
		if [ "$NEEDED" -ne "$CACHED" ]; then
			echo "$MYPATH has $NEEDED of $CACHED bytes"
			MET="false"
			break
		fi
	done	
	rm $OUTPUT
	if [ "$MET" == "true" ]; then
		break
	fi
done

echo "All caching is complete"
