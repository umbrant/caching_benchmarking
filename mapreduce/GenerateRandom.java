package com.cloudera;

import java.io.*;
import java.net.URI;
import java.security.InvalidParameterException;
import java.text.SimpleDateFormat;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class GenerateRandom {
  private static class SyntheticInputSplit extends InputSplit implements Writable {
    @Override
    public void readFields(DataInput arg0) throws IOException {
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
    }

    @Override
    public long getLength() throws IOException, InterruptedException {
      return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
      return new String[0];
    }
  }

  public static class RandomWordInputFormat extends
      InputFormat<Text, NullWritable> {

    public static final String DICTIONARY_FILE = "grand.dictionary.file";
    public static final String NUM_MAP_TASKS = "grand.num.map.tasks";
    public static final String WORDS_PER_TASK = "grand.words.per.task";

    @Override
    public List<InputSplit> getSplits(JobContext job) throws IOException {
      int numSplits = job.getConfiguration().getInt(NUM_MAP_TASKS, -1);
      if (numSplits <= 0) {
        throw new IOException(NUM_MAP_TASKS + " is not set.");
      }
      ArrayList<InputSplit> splits = new ArrayList<InputSplit>();
      for (int i = 0; i < numSplits; ++i) {
        splits.add(new SyntheticInputSplit());
      }
      return splits;
    }

    @Override
    public RecordReader<Text, NullWritable> createRecordReader(
            InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      String dictionaryFile = context.getConfiguration().get(DICTIONARY_FILE);
      if (dictionaryFile == null) {
        throw new IOException("must specify a " + DICTIONARY_FILE);
      }
      ArrayList<String> dictionary = loadDictionary(dictionaryFile);
      RandomWordRecordReader recordReader = new RandomWordRecordReader(dictionary);
      recordReader.initialize(split, context);
      return recordReader;
    }

    private ArrayList<String> loadDictionary(String dictionaryFile) throws IOException {
      BufferedReader reader = 
          new BufferedReader(new FileReader(dictionaryFile));
      try {
        ArrayList<String> words = new ArrayList<String>();
        while (true) {
          String line = reader.readLine();
          if (line == null) break;
          words.add(line);
        }
        if (words.isEmpty()) {
          throw new IOException("The dictionary was empty.");
        }
        return words;
      } finally {
        reader.close();
      }
    }

    public static void setNumMapTasks(Job job, int i) {
      job.getConfiguration().setInt(NUM_MAP_TASKS, i);
    }

    public static void setWordsPerTask(Job job, long i) {
      job.getConfiguration().setLong(WORDS_PER_TASK, i);
    }

    public static void setDictionary(Job job, String dictionaryFile) {
      job.getConfiguration().set(DICTIONARY_FILE, dictionaryFile);
    }

    public static class RandomWordRecordReader extends
        RecordReader<Text, NullWritable> {
      private final ArrayList<String> dictionary;
      private long wordsToWrite = 0;
      private long createdWords = 0;
      private final NullWritable value = NullWritable.get();
      private final Text key = new Text();
      private final Random random = new Random();

      RandomWordRecordReader(ArrayList<String> dictionary) {
        this.dictionary = dictionary;
      }

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context)
          throws IOException, InterruptedException {
        this.wordsToWrite =
            context.getConfiguration().getLong(WORDS_PER_TASK, -1);
        if (wordsToWrite < 0) {
          throw new IOException("must set " + WORDS_PER_TASK);
        }
      }

      @Override
      public boolean nextKeyValue() throws IOException,
          InterruptedException {
        // If we still have records to create
        if (createdWords < wordsToWrite) {
          key.set(dictionary.get(random.nextInt(dictionary.size())));
          ++createdWords;
          return true;
        } else {
          // Else, return false
          return false;
        }
      }

      @Override
      public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
      }

      @Override
      public NullWritable getCurrentValue()
            throws IOException, InterruptedException {
        return value;
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return (float)createdWords / (float)wordsToWrite;
      }

      @Override
      public void close() throws IOException {
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] remArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (remArgs.length != 4) {
      System.err.println(
          "Usage: GenerateRandom <numMaps> <wordsPerTask> <outputBase> <dictionaryFile>");
      System.exit(1);
    }
    int numMapTasks = Integer.parseInt(remArgs[0]);
    long wordsPerTask = Long.parseLong(remArgs[1]);
    Path outputBase = new Path(remArgs[2]);
    String dictionaryFile = remArgs[3];
    Job job = new Job(conf, "GenerateRandom");
    job.setJarByClass(GenerateRandom.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(RandomWordInputFormat.class);
    RandomWordInputFormat.setNumMapTasks(job, numMapTasks);
    RandomWordInputFormat.setWordsPerTask(job, wordsPerTask);
    RandomWordInputFormat.setDictionary(job, dictionaryFile);
    TextOutputFormat.setOutputPath(job, outputBase);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(NullWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
