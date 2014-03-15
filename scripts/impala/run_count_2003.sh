#!/bin/bash
if [ "$#" -ne 2 ]; then echo "$0 <number of runs> <output file>"; exit -1; fi

RUNS=$1
shift
OUTPUT=$1
shift

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Dropping caches across the cluster..."
../drop_caches.sh

# Clear the output file
echo -n > $OUTPUT

echo "Running $RUNS queries..."
# Collect output from 20 Impala runs
for i in `seq 1 $RUNS`; do
	impala-shell -i a2404.halxg.cloudera.com -f sql/count_store_sales_2003.sql 2>&1 | grep Returned >> $OUTPUT
done
echo "Finished!"
