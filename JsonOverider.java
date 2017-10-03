package project2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JsonOverider {

	public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
	    if (args.length != 2) {
	      System.err.println("Usage: problem2 <HDFS input file> <HDFS output file>");
	      System.exit(2);
	    }
	    Job job = new Job(conf, "JSON Problem");
	    job.setJarByClass(JsonOverider.class);
	    job.setInputFormatClass(JsonInputFormat.class);
	    job.setMapperClass(JsonMapper.class);
	    //job.setCombinerClass(JsonReducer.class);
	    job.setReducerClass(JsonReducer.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	public static class JsonMapper extends Mapper<Text,Text,IntWritable,IntWritable>{
		private final IntWritable values=new IntWritable(1);
		private IntWritable elevation=new IntWritable();
		
		public void map(Text key,Text value,Context context) throws IOException,InterruptedException{
			String [] jsonInput= value.toString().split(",");
			String  [] elevationValue = jsonInput[8].split(":");
			elevation.set(Integer.parseInt(elevationValue[1].trim().replaceAll("\"","")));
			context.write(elevation, values);
		}
	}
	public static class JsonReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable>{
		IntWritable count=new IntWritable();
		public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			int sum=0;
			Iterator<IntWritable> it=values.iterator();
			while(it.hasNext()){
				sum += it.next().get();
			}
			count.set(sum);
			context.write(key,count);
		}
	}
}
