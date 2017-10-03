package group9;

import java.io.IOException;
import java.lang.InterruptedException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Problem2 {

  public static class JSONInputFormat
       extends FileInputFormat<Object, Text> {

    @Override
    public RecordReader<Object, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
       throws IOException, InterruptedException {
      return new JSONRecordReader();
    }
  }

  public static class JSONRecordReader
       extends RecordReader<Object, Text> {

    LineRecordReader lineReader;
    Text value;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
      lineReader = new LineRecordReader();
      value = new Text();
      lineReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
      String line = "";
      String result = "";

      while (!line.equals("{")) {
        if (!lineReader.nextKeyValue()) { return false; }
        line = lineReader.getCurrentValue().toString().trim();
      }

      while (!line.replace(",","").equals("}")) {
        if (!lineReader.nextKeyValue()) { return false; }
        line = lineReader.getCurrentValue().toString().trim();
        String[] field = line.split(":");
        if (field.length == 2) {
          if (result.length() > 0) { result += ","; }
          result += field[0].replace("\"","") + ":" + field[1].replace("\"","").replace(",","").trim();
        }
      }

      value.set(result);
      return true;
    }

    @Override
    public LongWritable getCurrentKey() {
      return lineReader.getCurrentKey();
    }

    @Override
    public Text getCurrentValue() {
      return value;
    }

    @Override
    public float getProgress() throws IOException {
      return lineReader.getProgress();
    }

    @Override
    public void close() throws IOException {
      lineReader.close();
    }
  }

  // Source: Perera, Srinath. Hadoop MapReduce Cookbook, Packt Publishing, 2013.

  public static class P2Mapper
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text elevation = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] record = value.toString().split(",");
      for (String f : record) {
        String[] field = f.split(":");
        if (field[0].equals("Elevation")) {
          elevation.set(field[1]);
          context.write(elevation, one);
          break;
        }
      }
    }
  }
  
  public static class P2Reducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length != 2) {
      System.err.println("Usage: problem2 <HDFS input file> <HDFS output file>");
      System.exit(2);
    }
    Job job = new Job(conf, "problem 2");
    job.setJarByClass(Problem2.class);
    job.setInputFormatClass(JSONInputFormat.class);
    job.setMapperClass(P2Mapper.class);
    job.setCombinerClass(P2Reducer.class);
    job.setReducerClass(P2Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setNumReduceTasks(2);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
