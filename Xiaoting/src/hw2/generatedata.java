package hw2;


import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.FilterWriter;
import java.io.IOException;
import java.util.Random;


public class generatedata {
	public static Random numGen =new Random();
	
	

	public static int RandNum(){
		
	    int rand = Math.abs((1)+numGen.nextInt(10000));

	    return rand;
	}

	public static void main(String[]Args) throws IOException{
		FileWriter f0 = new FileWriter("/home/yifan/Desktop/datasample.txt");
		numGen.setSeed(1);
			
		for(int i=1;i<10000000;i++){
			String a=Integer.toString(RandNum());
			String b=Integer.toString(RandNum());
			String output = a+","+b+"\n";   //ans from other logic
		    
		    f0.write(output);
				
			
			//System.out.println(a+","+b);
		}
		f0.close();
	   
	}
		
	

}
