package com.cloudera;

import java.io.*;
import java.net.URI;
import java.security.InvalidParameterException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Map.Entry;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;

import org.apache.commons.lang.mutable.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ByteCount {
  public static class ByteCountMapper extends Mapper<LongWritable, Text, ByteWritable, LongWritable> {
    @Override
    public void map(LongWritable offset, Text text, Context context)
        throws IOException, InterruptedException {
      final int bytesLength = text.getLength();
      byte bytes[] = text.getBytes();
      HashMap<Byte, MutableInt> map = new HashMap<Byte, MutableInt>(8);
      for (int i = 0; i < bytesLength; i++) {
        Byte byteKey = Byte.valueOf(bytes[i]);
        MutableInt count = map.get(byteKey);
        if (count == null) {
          map.put(byteKey, new MutableInt(1));
        } else {
          count.increment();
        }
      }
      for (Entry<Byte, MutableInt> entry : map.entrySet()) {
        ByteWritable key = new ByteWritable(entry.getKey().byteValue());
        LongWritable value = new LongWritable(entry.getValue().intValue());
        context.write(key, value);
      }
    }
  }

  public static class ByteCountReducer
      extends Reducer<ByteWritable, LongWritable, ByteWritable, LongWritable> {
    protected void reduce(ByteWritable key, Iterable<LongWritable> values,
          Context context) throws IOException, InterruptedException {
      long count = 0;
      for (Iterator<LongWritable> iter = values.iterator();
          iter.hasNext(); ) {
        count += iter.next().get();
      }
      context.write(key, new LongWritable(count));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] remArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (remArgs.length != 2) {
      System.err.println(
          "Usage: ByteCount <inputBase> <outputBase>");
      System.exit(1);
    }
    String inputBase = remArgs[0];
    String outputBase = remArgs[1];
    Job job = new Job(conf, "ByteCount");
    job.setInputFormatClass(TextInputFormat.class);
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
