#!/bin/bash
set -e

if [ "$#" -ne 2 ]; then echo "$0 <number of runs> <output file>"; exit -1; fi

RUNS=$1
shift
OUTPUT=$1
shift

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Clear the output file
echo -n > $OUTPUT

echo "Running $RUNS queries..."
for i in `seq 1 $RUNS`; do
	../drop_caches.sh 2>&1
	impala-shell -i a2404.halxg.cloudera.com -f $DIR/sql/count_store_sales_2002-08-04.sql 2>&1 | grep Returned >> $OUTPUT
	echo "Query $i complete"
done
echo "Finished!"
