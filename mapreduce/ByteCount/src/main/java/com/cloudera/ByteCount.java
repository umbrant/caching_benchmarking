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
import java.nio.ByteBuffer;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ByteCount {
  public static class ByteCountMapper extends
      Mapper<LongWritable, ByteBufferWritable, ByteWritable, LongWritable> {
    @Override
    public void map(LongWritable offset, ByteBufferWritable bufferWritable,
        Context context) throws IOException, InterruptedException {
      ByteBuffer buf = bufferWritable.getBuffer();
      final int bytesLength = buf.limit();
      long[] counts = new long[256];
      for (int i = 0; i < bytesLength; i++) {
        int value = buf.get() & 0xFF;
        counts[value]++;
      }
      for (int i=0; i<counts.length; i++) {
        ByteWritable key = new ByteWritable((byte)i);
        LongWritable value = new LongWritable(counts[i]);
        context.write(key, value);
      }
    }
  }

  public static class ByteCountReducer extends
      Reducer<ByteWritable, LongWritable, ByteWritable, LongWritable> {
    protected void reduce(ByteWritable key, Iterable<LongWritable> values,
        Context context) throws IOException, InterruptedException {
      long count = 0;
      for (Iterator<LongWritable> iter = values.iterator(); iter.hasNext();) {
        count += iter.next().get();
      }
      context.write(key, new LongWritable(count));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] remArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (remArgs.length != 2) {
      System.err.println("Usage: ByteCount <inputBase> <outputBase>");
      System.exit(1);
    }
    String inputBase = remArgs[0];
    String outputBase = remArgs[1];
    Job job = new Job(conf, "ByteCount");
    job.setInputFormatClass(ByteBufferInputFormat.class);

    job.setMapOutputKeyClass(ByteWritable.class);
    job.setMapOutputValueClass(LongWritable.class);

    job.setMapperClass(ByteCountMapper.class);
    job.setReducerClass(ByteCountReducer.class);
    job.setCombinerClass(ByteCountReducer.class);
    
    job.setOutputKeyClass(ByteWritable.class);
    job.setOutputValueClass(LongWritable.class);

    FileInputFormat.addInputPath(job, new Path(inputBase));
    FileOutputFormat.setOutputPath(job, new Path(outputBase));

    job.setJarByClass(ByteCount.class);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
