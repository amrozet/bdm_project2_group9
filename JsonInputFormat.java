package project2;


import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

public class JsonInputFormat extends  FileInputFormat<Text,Text>{
	public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new RecordReaderOverider();
	}
}
class RecordReaderOverider extends RecordReader<Text,Text>{
	private LineRecordReader lineRecordReader = null;
    private Text key = null;
    private Text value = null;
    private BufferedReader in;
   
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		 /*if (null != lineRecordReader) {
             lineRecordReader.close();
             lineRecordReader = null;
         }*/
         key = null;
         value = null;
         in.close();
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return key;
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//return lineRecordReader.getProgress();
		return 0;
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		FileSplit split = (FileSplit) arg0;
		Configuration conf = arg1.getConfiguration();
		Path path = split.getPath();
		FileSystem fs = path.getFileSystem(conf);
		FSDataInputStream fsInput = fs.open(path);
		in = new BufferedReader(new InputStreamReader(fsInput));
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String line = in.readLine();
		StringBuffer sb=new StringBuffer();
		if(line == null){
			return false;
		}else{
			while((line = in.readLine())!=null){
				if(line.trim().contains("{")){
					continue;
				}
				if(line.trim().contains("}")){
					break;
				}
				sb.append(line.trim().replaceAll("\"", ""));
			}
			key=new Text(String.valueOf(sb.toString().length()));
			value=new Text(sb.toString());
			return true;
		}
	}
	
}
