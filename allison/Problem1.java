package group9;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.HashMap;
import java.util.ArrayList;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Problem1 {

  public static class P1Mapper
       extends Mapper<Object, Text, Text, Text>{
    
    private Text rectID = new Text(); 
    private HashMap<String,ArrayList<Integer>> rectangles;
    private String window;
    private ArrayList<Integer> windCorners;

    // load small rectangles file
    public void setup(Context context) throws IOException, InterruptedException {
      rectangles = new HashMap<String,ArrayList<Integer>>();
      windCorners = new ArrayList<Integer>(4);

      super.setup(context);
      Configuration configuration = context.getConfiguration();
      window = configuration.get("Settings.Window").replace("W","").replace("(","").replace(")","");
      String[] window_split = window.split(",");
      for (int i=0; i<window_split.length; i++) {
        windCorners.add(Integer.parseInt(window_split[i].trim()));
      }

      URI[] uris = DistributedCache.getCacheFiles(configuration);
      if (uris != null) {
        FSDataInputStream input_stream = FileSystem.get(configuration).open(new Path(uris[0]));
        BufferedReader bReader = new BufferedReader(new InputStreamReader(input_stream));
        String record_line = bReader.readLine();
        while (record_line != null) {
          String[] records = record_line.split(",");
          ArrayList<Integer> corners = new ArrayList<Integer>(4);
          for (int i=1; i<records.length; i++) {
            corners.add(Integer.parseInt(records[i]));
          }
          rectangles.put(records[0], corners);
          record_line = bReader.readLine();
        }
        bReader.close();
      }
    }
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //ArrayList<String> temp = new ArrayList<String>();
      String[] coords = value.toString().split(",");
      Integer x = Integer.parseInt(coords[0]);
      Integer y = Integer.parseInt(coords[1]);
      if (x>=windCorners.get(0) && x<=windCorners.get(2) && y>=windCorners.get(1) && y<=windCorners.get(3)) {
        for (String rect : rectangles.keySet()) {
          ArrayList<Integer> corners = rectangles.get(rect);
          if (x>=corners.get(0) && x<=corners.get(2) && y>=corners.get(1) && y<=corners.get(3)) {
            //temp.add(rect);
            rectID.set(rect);
            context.write(rectID, value);
          }
        }
      }
    }

  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    if (args.length < 3) {
      System.err.println("Usage: problem1 <HDFS input file> <HDFS input file> <HDFS output file> <optional window>");
      System.exit(2);
    }

    DistributedCache.addCacheFile(new URI(args[0]), conf);
    if (args.length == 4) { conf.set("Settings.Window", args[3]); }
    else { conf.set("Settings.Window", "W(1,1,10000,10000)"); }

    Job job = new Job(conf, "problem 1");
    job.setJarByClass(Problem1.class);
    job.setMapperClass(P1Mapper.class);
    //job.setCombinerClass(TaskBReducer.class);
    //job.setReducerClass(P1Reducer.class);
    job.setOutputKeyClass(Text.class);
    job.setNumReduceTasks(0);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[1]));
    FileOutputFormat.setOutputPath(job, new Path(args[2]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
