/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CacheFlag;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveEntry;
import org.apache.hadoop.hdfs.protocol.CacheDirectiveInfo;

public class CacheTool {

  static Configuration conf;
  static HdfsAdmin admin;

  private static void usage() {
    System.out.println("CacheTool removeAll");
    System.out.println("CacheTool cache <amount>");
  }

  private static void removeAll() throws IOException {
    // Get all the IDs
    RemoteIterator<CacheDirectiveEntry> it = admin.listCacheDirectives(null);
    ArrayList<Long> ids = new ArrayList<Long>();
    while (it.hasNext()) {
      ids.add(it.next().getInfo().getId());
    }
    System.out.println("Removing " + ids.size() + " directives...");
    // Remove all the IDs
    for (Long id : ids) {
      System.out.println("Removing directive " + id);
      admin.removeCacheDirective(id);
    }
  }

  private static void cache(final long total) throws IllegalArgumentException,
      IOException {
    // Count downward so our WHERE queries can be >=

    // Calculate how many partitions we need
    long bytes = 0;
    int count = 0;
    for (int i = Constants.SIZES.length - 1; i >= 0; i--) {
      long partitionBytes = Constants.SIZES[i];
      if (bytes + partitionBytes > total) {
        break;
      }
      count++;
      bytes += partitionBytes;
    }

    System.out.println("Need " + count + " partitions, caching " + bytes
        + " bytes");
    // Cache the # of partitions
    for (int i = Constants.DIRS.length - 1 - count; i < Constants.DIRS.length - 1; i++) {
      String path = Constants.DIRS[i];
      System.out.println("Adding directive for path " + path);
      admin.addCacheDirective(
          new CacheDirectiveInfo.Builder().setPath(new Path(path))
              .setPool("pool1").build(), EnumSet.noneOf(CacheFlag.class));
    }
  }

  public static void main(String[] args) throws Exception {

    if (args.length == 0) {
      usage();
      System.exit(-1);
    }

    conf = new Configuration();
    admin = new HdfsAdmin(FileSystem.getDefaultUri(conf), conf);

    String command = args[0];
    if (command.equals("removeAll")) {
      removeAll();
    } else if (command.equals("cache")) {
      if (args.length != 2) {
        usage();
        System.exit(1);
      }
      final long needed = Long.parseLong(args[1]);
      cache(needed);
    } else {
      usage();
      System.exit(-2);
    }

    System.out.println("Done!");
    System.exit(0);
  }
}
