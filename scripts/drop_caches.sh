#!/bin/bash
pssh -p4 -i -h hosts.txt "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches"
