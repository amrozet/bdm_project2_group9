package group9;

import java.util.Random;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
//import java.util.HashMap;
//import java.util.HashSet;

public class Main1 {

	public static void main(String[] args) {
		String file_P = "";
		String file_R = "";
		
		int numPoints = 10;
		int numRectangles = 6;
		int space = 10000;
		
		for (int i=0; i<args.length; i++) {
			if (args[i].equals("-P")) file_P = args[++i];
			if (args[i].equals("-R")) file_R = args[++i];
			
			if (args[i].equals("-np")) numPoints = Integer.parseInt(args[++i]);
			if (args[i].equals("-nr")) numRectangles = Integer.parseInt(args[++i]);
		}
		
		Random random = new Random();
		/* 
		 * Given that the real world is 3D, but we are only specifying x and y coordinates,
		 * we do NOT require that each point (rectangle) be distinct. It is possible, say, that
		 * Point p1 is parked on the first floor of a parking garage and Point p2 is on the second
		 * floor, but both points share the exact same x and y coordinates.
		 * Likewise, Rectangles r1 and r2 denoting the first and second floors of the same parking garage
		 * may share the same boundaries.
		 * ?????
		 */
		
		/*
		 * We also assume that points and rectangles can lie on the edge of the space, i.e. at
		 * values 1 or 10,000.
		 */
		
		// Points: x,y
		try {			 
			File output_file = new File(file_P);
			BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
			
			for (int i=0; i<numPoints; i++) {
				int x = random.nextInt(space)+1;
				int y = random.nextInt(space)+1;
				output.append(x + "," + y + "\n");
			}
			output.close();
		} catch (IOException e) { e.printStackTrace(); }
		
		// Rectangles: id,x-bottom-left,y-bottom-left,x-top-right,y-top-right
		try {			 
			File output_file = new File(file_R);
			BufferedWriter output = new BufferedWriter(new FileWriter(output_file));
			
			for (int i=0; i<numRectangles; i++) {
				// Set bottom left corner at most (9998, 9998) to allow for a meaningful rectangle.
				int[] coords = new int[] {random.nextInt(space-2)+1, random.nextInt(space-2)+1, 0, 0};
				
				// Are the rectangles too big???
				int width = random.nextInt(space-coords[0])+1;
				int height = random.nextInt(space-coords[1])+1;
				
				if (width > 20) { width = random.nextInt(20)+1; }
				if (height > 20) { height = random.nextInt(20)+1; }
				
				coords[2] = coords[0] + width;
				coords[3] = coords[1] + height;
				
				output.append(i+1 + "," + coords[0] + "," + coords[1] + "," + coords[2] + "," + coords[3] + "\n");
			}
			output.close();
		} catch (IOException e) { e.printStackTrace(); }

	}

}
