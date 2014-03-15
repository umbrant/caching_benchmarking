#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Invalidating and refreshing store sales metadata"
impala-shell -i a2404.halxg.cloudera.com -f $DIR/sql/invalidate_store_sales.sql

echo "Finished!"
