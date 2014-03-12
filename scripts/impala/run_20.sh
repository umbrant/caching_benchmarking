#!/bin/bash
if [ $# != 1 ]; do "Must provide output filename"; exit -1; done
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Start by dropping caches across the cluster
../drop_caches.sh

# Clear the output file
echo -n > $1

# Collect output from 20 Impala runs
for x in {1..20}; do
	impala-shell -i a2404.halxg.cloudera.com -f sql/count_store_sales_2003.sql 2>&1 | grep Returned >> $1
done
