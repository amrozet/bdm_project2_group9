package project2;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DCMap extends Mapper<LongWritable,Text,Text,Text>{
	    private Text word = new Text();
	    private Text value=new Text();
	    private Set stopWords = new HashSet();
	    @Override
	    protected void setup(Context context) throws IOException, InterruptedException {
	        try{
	            Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
	            if(stopWordsFiles != null && stopWordsFiles.length > 0) {
	                for(Path stopWordFile : stopWordsFiles) {
	                    readFile(stopWordFile);
	                }
	            }
	        } catch(IOException ex) {
	            System.err.println("Exception in mapper setup: " + ex.getMessage());
	        }
	    }
	    @Override
	    protected void map(LongWritable key, Text value,
	            Context context)
	            throws IOException, InterruptedException {
	        String [] line = value.toString().split(",");
	        Iterator<String> iterator=stopWords.iterator();
	        int x1=0,x2=0,y1=0,y2=0;
	        int x=Integer.parseInt(line[0]);
	        int y=Integer.parseInt(line[1]);
	        while(iterator.hasNext()){
	        	String [] temp= iterator.next().trim().split(",");
	        	x1=Integer.parseInt(temp[1]);
	        	x2=Integer.parseInt(temp[3]);
	        	y1=Integer.parseInt(temp[2]);
	        	y2=Integer.parseInt(temp[4]);
	        	
	        	if((x >= x1 && x <= x2) && (y >= y1 && y <= y2)){
	        		word.set(temp[0]);
	        		value.set(x+","+y);
	        		context.write(word, value);
	        	}
	        }
	        
	    }
	    private void readFile(Path filePath) {
	        try{
	            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
	            String line = null;
	            while((line = bufferedReader.readLine()) != null) {
	                stopWords.add(line.trim());
	            }
	        } catch(IOException ex) {
	            System.err.println("Exception while reading stop words file: " + ex.getMessage());
	        }
	    }

}
