#!/bin/bash
sudo -u hdfs hdfs cacheadmin -addDirective -pool pool1 -path /wikipedia_snapshots/dewiki-20121117-pages-meta-current.xml -replication 3
