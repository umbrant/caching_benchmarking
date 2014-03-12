#!/bin/bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd $DIR
pssh -p4 -i -h hosts.txt "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
