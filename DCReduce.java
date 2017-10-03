package project2;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class DCReduce extends Reducer<Text,Text,Text,Text> {
	protected void reduce(Text key, Iterable values,Context context)
            throws IOException, InterruptedException {
        Configuration con=new Configuration();
        String [] inputParam=context.getConfiguration().get("inputparameter").trim().substring(2, 10).split(",");
        int x1=Integer.parseInt(inputParam[0]);
        int y1=Integer.parseInt(inputParam[1]);
        int x2=Integer.parseInt(inputParam[2]);
        int y2=Integer.parseInt(inputParam[3]);
        
        Iterator valuesIt = values.iterator();
        while(valuesIt.hasNext()){
            Text temp=(Text) valuesIt.next();
            String [] mapOutput=temp.toString().trim().split(",");
            int x= Integer.parseInt(mapOutput[0]);
            int y=Integer.parseInt(mapOutput[1]);
            
            if((x >= x1 && x <= x2) && (y >= y1 && y <= y2)){
            	Text reduceOutput=new Text(x+","+y);
            	context.write(key, reduceOutput);
            }
        }
    }  
}