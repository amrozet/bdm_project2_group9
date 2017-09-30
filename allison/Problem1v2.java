package group9;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.util.HashSet;
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

public class Problem1v2 {

  public static class P1Mapper
       extends Mapper<Object, Text, Text, Text>{
    
    private Text rectID = new Text(); 
    private HashSet<ArrayList<Integer>> points;
    private String window;
    private ArrayList<Integer> windCorners;

    // load small points file
    public void setup(Context context) throws IOException, InterruptedException {
      points = new HashSet<ArrayList<Integer>>();
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
          ArrayList<Integer> coords = new ArrayList<Integer>(2);
          for (int i=0; i<records.length; i++) {
            coords.add(Integer.parseInt(records[i]));
          }
          points.add(coords);
          record_line = bReader.readLine();
        }
        bReader.close();
      }
    }
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //ArrayList<String> temp = new ArrayList<String>();
      String[] corners = value.toString().split(",");

      for (ArrayList<Integer> p : points) {
        int x = p.get(0);
        int y = p.get(1);
        if (x>=windCorners.get(0) && x<=windCorners.get(2) && y>=windCorners.get(1) && y<=windCorners.get(3)) {
          if (x>=Integer.parseInt(corners[1]) && x<=Integer.parseInt(corners[3]) && y>=Integer.parseInt(corners[2]) && y<=Integer.parseInt(corners[4])) {
            //temp.add(rect);
            rectID.set(corners[0]);
            context.write(rectID, new Text(x+","+y));
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

    Job job = new Job(conf, "problem 1 version 2");
    job.setJarByClass(Problem1v2.class);
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
