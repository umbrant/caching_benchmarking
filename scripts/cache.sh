#!/bin/bash

set -e

sudo -u hdfs hadoop jar ./CacheTool/target/cachetool-1.0-SNAPSHOT.jar com.cloudera.CacheTool cache $1
