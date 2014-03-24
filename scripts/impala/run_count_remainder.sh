#!/bin/bash
set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Looping remainder query forever..."
while true; do
	impala-shell -i a2404.halxg.cloudera.com -f $DIR/sql/count_store_sales_remainder.sql 2>&1 > /dev/null
done
echo "Finished!"
