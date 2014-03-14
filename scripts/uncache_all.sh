#!/bin/bash

set -e

OUTPUT="/tmp/wait-cache-$$"

IFS='
'
for directive in `hdfs cacheadmin -listDirectives | tail -n +3`; do
	ID=`echo "$directive" | awk '{print $1}'`
	MYPATH=`echo "$directive" | awk '{print $5}'`
	echo "Uncaching directive $ID path ${MYPATH}"
	sudo -u hdfs hdfs cacheadmin -removeDirective $ID
done

echo "All uncached"
