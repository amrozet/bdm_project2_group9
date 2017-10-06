package hw2;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class optallpoint {
	public static String CP_int_path = "/home/yifan/hw2/KMean";
	public static String OUTPUT_FILE_NAME = "/part-r-00000";
	static long startTime;

	// create and store center points
	private static void CreateCenterPoints(int kseeds) throws IOException,
			URISyntaxException {
		FileSystem hdfs = FileSystem.get(new Configuration());
		Path file = new Path(CP_int_path);
		if (hdfs.exists(file)) {
			hdfs.delete(file, true);
		}
		BufferedWriter br = new BufferedWriter(new OutputStreamWriter(
				hdfs.create(file, true)));
		Random random = new Random();
		random.setSeed(100);
		float x, y;
		for (int i = 0; i < kseeds; i++) {
			x = random.nextFloat() * 10000;
			y = random.nextFloat() * 10000;
			String line = Float.toString(x) + "," + Float.toString(y) + "\r\n";
			br.write(line);
		}
		br.close();
		hdfs.close();
	}

	public static class KDisctanceMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private float Px = 0;
		private float Py = 0;
		private ArrayList<Float> Cx = new ArrayList<Float>();
		private ArrayList<Float> Cy = new ArrayList<Float>();
		private Text outputKey = new Text();
		private Text outputValue = new Text();

		BufferedReader brReader;

		enum MYCOUNTER {
			RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
		}

		protected void setup(Context context) throws IOException,
				InterruptedException { // read center points

			try {
				URI[] urilist=DistributedCache.getCacheFiles(context.getConfiguration());
				brReader=new BufferedReader(new FileReader(urilist[0].getPath()));
				/*Path[] cacheFilesLocal = DistributedCache
						.getLocalCacheFiles(context.getConfiguration());
				brReader = new BufferedReader(new FileReader(
						cacheFilesLocal[0].toString()));*/
				String strLineRead = "";
				while ((strLineRead = brReader.readLine()) != null) {
					String CenterArray[] = strLineRead.split(",");
					Cx.add(Float.parseFloat(CenterArray[0].trim()));
					Cy.add(Float.parseFloat(CenterArray[1].trim()));
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
			} catch (IOException e) {
				context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
				e.printStackTrace();
			} finally {
				if (brReader != null) {
					brReader.close();
				}
			}

		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] line_splits = line.split(",");

			Px = Float.parseFloat((line_splits[0]));
			Py = Float.parseFloat((line_splits[1]));

			float distance = 0.0f;
			float mindistance = 2.0f * 10000.0f * 10000.0f; // maximimun
															// distance in the
															// dataset
			int winnercenter = -1;
			int i = 0;

			for (i = 0; i < Cx.size(); i++) {
				distance = (float) (Math.pow(Px - Cx.get(i), 2) + Math.pow(Py
						- Cy.get(i), 2));
				if (distance < mindistance) {
					mindistance = distance;
					winnercenter = i;
				}
			}

			outputKey.set(Cx.get(winnercenter) + "," + Cy.get(winnercenter));
			outputValue.set(Px + "," + Py+","+"1");

			context.write(outputKey, outputValue);// centerpoint, kpoint
		}
	}
	
	
	/*public static class kmeancombiner 
	 extends Reducer<Text, Text,Text,Text> {
		  
		  
		  public void reduce(Text key, Iterable<Text> values , 
	                 Context context
	                 ) throws IOException, InterruptedException {
			  float centerx = 0.0f;
				float centery = 0.0f;
				int num = 0;
				String[] splits;
				System.err.println("combiner");

				for (Text val : values) {
					num++;
					splits = val.toString().split(",");
					centerx += Float.parseFloat(splits[0]);
					centery += Float.parseFloat(splits[1]);
				}
			  Text results = new Text();
			  String number=Integer.toString(num);
			  
			  results.set(Float.toString(centerx)+","+Float.toString(centery)+","+num);
			  
			  
			  context.write(key,results);
		}
		  
		  
			  
	 
	 }*/

	public static class KCenterReducer extends Reducer<Text, Text, Text, Text> {
		private Text newCenter = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			float centerx = 0.0f;
			float centery = 0.0f;
			int num = 0;
			String[] splits;
			StringBuilder sb = new StringBuilder();

			for (Text val : values) {
				//num++;
				//System.err.println(val);
				splits = val.toString().split(",");
				centerx += Float.parseFloat(splits[0]);
				centery += Float.parseFloat(splits[1]);
				num+=Integer.parseInt(splits[2]);
				String line=splits[0]+","+splits[1];
				//System.err.println(line);
				sb.append(line).append(";");
			}
			centerx = centerx / num;
			centery = centery / num;
			newCenter.set(centerx + "," + centery+",");
			Text results = new Text();
			results.set(sb.substring(0,sb.length()-1));
			context.write(newCenter, results); 
		}
	}

	public static class DriverKMeans extends Configured implements Tool {
		public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err
					.println("Usage: <number of k> <HDFS input file> <HDFS output file>");
			System.exit(3);
		}

		String input = args[1]; // input path (dataset)
		String output = args[2] + "_"+"0"; // output path, add time
														// --> unique file name
		String again_input = output;

		boolean success = false; 
		
		// set maximum number of iterations:6
		for (int i = 0; i < 6; i++) {
			System.out.println(i);
			Configuration conf = new Configuration();

			if (i == 0) {
				CreateCenterPoints(Integer.parseInt(args[0])); // k number
				DistributedCache.addCacheFile(new URI(CP_int_path), conf);
			} else {
				DistributedCache.addCacheFile(new URI(again_input
						+ OUTPUT_FILE_NAME), conf);// the last output is new
													// center for second loop
			}

			Job job = new Job(conf, "problem3");
			job.setJarByClass(optallpoint.class);
			job.setMapperClass(KDisctanceMapper.class);
			job.setReducerClass(KCenterReducer.class);
			job.setNumReduceTasks(1);
			//job.setCombinerClass(kmeancombiner.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, new Path(input)); // k-means
																// points
																// dataset
			FileOutputFormat.setOutputPath(job, new Path(output)); // give a new
																	// center
			success = job.waitForCompletion(true);
			
			// get new and old center points to make a comparison
			// get new center points
			FileSystem hdfs = FileSystem.get(new Configuration());
			Path file = new Path(output + OUTPUT_FILE_NAME); // second output
			BufferedReader br_new = new BufferedReader(new InputStreamReader(
					hdfs.open(file)));
			ArrayList<Float> C_new_x = new ArrayList<Float>();
			ArrayList<Float> C_new_y = new ArrayList<Float>();
			String line = "";
			while ((line = br_new.readLine()) != null) {
				String split[] = line.split(",");
				C_new_x.add(Float.parseFloat(split[0]));// maybe cannot get new
														// points
				C_new_y.add(Float.parseFloat(split[1]));
			}
			br_new.close();
			
			String prev;
			if (i == 0) {
				prev = CP_int_path;
			} else {
				prev = again_input + OUTPUT_FILE_NAME; // for second time, first
														// output is the old one
			}

			FileSystem hdfs2 = FileSystem.get(new Configuration());
			Path file2 = new Path(prev);
			BufferedReader br_old = new BufferedReader(new InputStreamReader(
					hdfs2.open(file2)));
			ArrayList<Float> C_old_x = new ArrayList<Float>();
			ArrayList<Float> C_old_y = new ArrayList<Float>();
			String line2 = "";
			while ((line2 = br_old.readLine()) != null) {
				String split[] = line2.split(",");
				C_old_x.add(Float.parseFloat(split[0]));// maybe cannot get old
														// points
				C_old_y.add(Float.parseFloat(split[1]));
			}
			br_old.close();
			
			Collections.sort(C_new_x);
			Collections.sort(C_new_y);
			Collections.sort(C_old_x);
			Collections.sort(C_old_y);

			float temp_sum = 0.0f;
			for (int j = 0; j < C_new_x.size(); j++) {
				temp_sum += Math.pow(C_new_x.get(j) - C_old_x.get(j), 2)
						+ Math.pow(C_new_y.get(j) - C_old_y.get(j), 2);
			}
			System.out.println("sum error:"+temp_sum);
			if (temp_sum < 100) {
				break;
			}
			again_input = output;
			output = args[2] + "_" + (i+1);
		}
		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.err.println(totalTime);
		return success ? 0 : 1;
		}
		
	}
	
	public static void main(String args[]) throws Exception {
		startTime = System.currentTimeMillis();
		int exitCode = ToolRunner.run(new Configuration(),
				new DriverKMeans(), args);
		System.exit(exitCode);
	}
}