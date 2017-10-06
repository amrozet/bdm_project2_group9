package group9;

import java.util.Random;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Main3 {

	public static void main(String[] args) {
		String file_P = "";
		
		int k = 3; // number of clusters
		int m = 100; // number of points per cluster
		int space = 10000;
		
		for (int i=0; i<args.length; i++) {
			if (args[i].equals("-P")) file_P = args[++i];			
			if (args[i].equals("-k")) k = Integer.parseInt(args[++i]);
			if (args[i].equals("-m")) m = Integer.parseInt(args[++i]);
		}
		
		int size = space/20;
		
		Random random = new Random();
		/* 
		 * We allow duplicates, i.e. each point need not be distinct.
		 * We also assume that points and rectangles can lie on the edge of the space, i.e. at 10,000.
		 */
		
		// Points: x,y
		try {			 
			File output_file = new File(file_P);
			BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
			
			for (int i=0; i<k; i++) {
				int x_c = random.nextInt(space);
				int y_c = random.nextInt(space);
				for (int j=0; j<m; j++) {
					int r = random.nextInt(size);
					int s = size - r;
					int x = x_c - r;
					x += random.nextInt(2*r+1);
					int y = y_c - s;
					y += random.nextInt(2*s+1);
					if (x<0) { x=0; }
					if (x>space) { x=space; }
					if (y<0) { y=0; }
					if (y>space) { y=space; }
					output.append(x + "," + y + "\n");
				}
			}
			output.close();
		} catch (IOException e) { e.printStackTrace(); }

	}

}
