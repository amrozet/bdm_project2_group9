package project2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Driver extends Configured implements Tool{
   public static void main(String[] args) throws Exception{
        int exitCode = ToolRunner.run(new Driver(), args);
       System.exit(exitCode);
	   }
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.printf("Usage: %s needs two arguments    files\n",
                    getClass().getSimpleName());
            return -1;
        }
        Configuration conf=new Configuration();
        conf.set("inputparameter", "W(1,3,3,20)");
        Job job = new Job(conf);
        job.setJarByClass(Driver.class);
        job.setJobName("Word Counter With Stop Words Removal");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapperClass(DCMap.class);
        job.setReducerClass(DCReduce.class);
        DistributedCache.addCacheFile(new Path(args[1]).toUri(), job.getConfiguration());
        int returnValue = job.waitForCompletion(true) ? 0:1;
        if(job.isSuccessful()) {
            System.out.println("Job was successful");
        } else if(!job.isSuccessful()) {
            System.out.println("Job was not successful");          
        }
         
        return returnValue;
    }
}
