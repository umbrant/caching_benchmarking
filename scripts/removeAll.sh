#!/bin/bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

sudo -u hdfs hadoop jar $DIR/CacheTool/target/cachetool-1.0-SNAPSHOT.jar com.cloudera.CacheTool removeAll
