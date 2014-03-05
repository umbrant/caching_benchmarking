#!/bin/bash
pssh -p4 -i -h hosts.txt "echo 1 | sudo tee /proc/sys/vm/drop_caches"
