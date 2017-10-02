package group9;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
//import java.util.HashSet;
import java.util.ArrayList;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Problem1v3 {

  // "stripe" by x value
  // worked for small data set, but not large data set
  // partition again y > 5000 (top) and y <= 5000 (bottom)

  // input: points
  // output: <(x value:top/bottom), y value>
  public static class PointMapper
       extends Mapper<Object, Text, Text, Text>{
    
    private Text xval = new Text(); 
    private Text yval = new Text();
    private String window;
    private ArrayList<Integer> windCorners;

    // load window
    public void setup(Context context) throws IOException, InterruptedException {
      windCorners = new ArrayList<Integer>(4);

      super.setup(context);
      Configuration configuration = context.getConfiguration();
      window = configuration.get("Settings.Window").replace("W","").replace("(","").replace(")","");
      String[] window_split = window.split(",");
      for (int i=0; i<window_split.length; i++) {
        windCorners.add(Integer.parseInt(window_split[i].trim()));
      }
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] coords = value.toString().split(",");
      int xcoord = Integer.parseInt(coords[0]);
      int ycoord = Integer.parseInt(coords[1]);
      if (xcoord>=windCorners.get(0) && xcoord<=windCorners.get(2) && ycoord>=windCorners.get(1) && ycoord<=windCorners.get(3)) {
        if (Integer.parseInt(coords[1])>5000) {
          xval.set(coords[0] + ":" + "top");
        } else {
          xval.set(coords[0] + ":" + "bottom");
        }
        yval.set(coords[1]);
        context.write(xval, yval);
      }
    }
  }

  // input: rectangles
  // output: <(x value:top/bottom, rid), y values>
  public static class RectMapper
       extends Mapper<Object, Text, Text, Text>{
    
    private Text xval = new Text(); 
    private Text yvals = new Text();
    private String window;
    private ArrayList<Integer> windCorners;

    // load window
    // although not required for the correct result (PointMapper filters the points),
    // we chose to also filter rectangles to speed up shuffle and sort
    public void setup(Context context) throws IOException, InterruptedException {
      windCorners = new ArrayList<Integer>(4);

      super.setup(context);
      Configuration configuration = context.getConfiguration();
      window = configuration.get("Settings.Window").replace("W","").replace("(","").replace(")","");
      String[] window_split = window.split(",");
      for (int i=0; i<window_split.length; i++) {
        windCorners.add(Integer.parseInt(window_split[i].trim()));
      }
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String[] corners = value.toString().split(",");
      int xleft = Integer.parseInt(corners[1]);
      int ybottom = Integer.parseInt(corners[2]);
      int xright = Integer.parseInt(corners[3]);
      int ytop = Integer.parseInt(corners[4]);
      if (xright>=windCorners.get(0) && xleft<=windCorners.get(2) && ytop>=windCorners.get(1) && ybottom<=windCorners.get(3)) {
        for (int i=Integer.parseInt(corners[1]); i<=Integer.parseInt(corners[3]); i++) {
          yvals.set(corners[2] + "," + corners[4]);
          if (Integer.parseInt(corners[4]) > 5000) {
            xval.set(i+ ":" +"top" + "," + corners[0]);
            context.write(xval, yvals);
          }
          if (Integer.parseInt(corners[2]) <= 5000) {
            xval.set(i+ ":" + "bottom" + "," + corners[0]);
            context.write(xval, yvals);
          }
        }
      }
    }
  }

  // partitioner decides which reducer you get sent to
  public static class P1Partitioner
       extends Partitioner<Text, Text> {

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
      String[] myKey = key.toString().split(",");
      return Math.abs(myKey[0].hashCode()) % numPartitions;
      // absolute value is required because hashCode() sometimes returns negative value
    }
  }

  // grouping comparator
  public static class P1Grouping
       extends WritableComparator {

    public P1Grouping() {
      super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable k1, WritableComparable k2) {
      Text key1 = (Text) k1;
      Text key2 = (Text) k2;
      String[] myKey1 = k1.toString().split(",");
      String[] myKey2 = k2.toString().split(",");
      return myKey1[0].compareTo(myKey2[0]);
    }
  }

  // after performing a few experiments, I determined that each x value primary key
  // will receive about as twice as many rectangles as points. thus we
  // sort points before rectangle
  public static class P1Sort
     extends WritableComparator {

    public P1Sort() {
      super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable k1, WritableComparable k2) {
      Text key1 = (Text) k1;
      Text key2 = (Text) k2;
      String[] myKey1 = k1.toString().split(",");
      String[] myKey2 = k2.toString().split(",");
      int compareResult = myKey1[0].compareTo(myKey2[0]);

      if (compareResult == 0) {
        return myKey1.length - myKey2.length;
      }
      return compareResult;
    }
  }

  public static class P1Reducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text rectID = new Text();
    private Text coords = new Text();
    private ArrayList<Integer> ys;

    // all points will be read in first
    // assume all points will fit in memory and store
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      //int t = 0;
      ys = new ArrayList<Integer>();
      for (Text val : values) {
        String[] keys = key.toString().split(",");
        if (keys.length == 1) {
          // Point
          ys.add(Integer.parseInt(val.toString()));
        } else if (keys.length > 1) {
          // Rectangle
          String[] vals = val.toString().split(",");
          int low = Integer.parseInt(vals[0]);
          int high =  Integer.parseInt(vals[1]);
          for (Integer ypoint : ys) {
            if (ypoint >= low && ypoint <= high) {
              rectID.set(keys[1]);
              String[] stripe = keys[0].toString().split(":");
              coords.set(stripe[0] + "," + ypoint);
              context.write(rectID, coords);
            }
          }
        }
        // debug
        //System.out.println("Time: " + t + "; Key: " + key.toString() + "; Value: " + val.toString());
        //t++;
      }
    }
  }


  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length < 3) {
      System.err.println("Usage: problem1 <HDFS points input file> <HDFS rectangles input file> <HDFS output file> <optional window>");
      System.exit(2);
    }

    DistributedCache.addCacheFile(new URI(args[0]), conf);
    if (args.length == 4) { conf.set("Settings.Window", args[3]); }
    else { conf.set("Settings.Window", "W(1,1,10000,10000)"); }

    Job job = new Job(conf, "problem 1 version 3");
    job.setJarByClass(Problem1v3.class);
    MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, PointMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RectMapper.class);
    job.setPartitionerClass(P1Partitioner.class);
    job.setGroupingComparatorClass(P1Grouping.class);
    job.setSortComparatorClass(P1Sort.class);
    job.setReducerClass(P1Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setNumReduceTasks(3);
    job.setOutputValueClass(Text.class);
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
