package com.visa.json.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class StripLinesFromJSON {
	
	public static final int INDEX_NOT_FOUND = -1;
	
	public StripLinesFromJSON() {
	}
	
	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.err.println("Usage: StripLinesfromJSON <inputpath/inputfile> <outputpath/outputfile");
			System.exit(1);
		}
		
		StripLinesFromJSON sl = new StripLinesFromJSON();
		sl.strip(args[0], args[1]);
	}
	
	private void strip(String inputPath, String outputPath) throws IOException {
		
		long lines = 0;
		long start = System.currentTimeMillis();
		StringBuffer curRec = new StringBuffer();
		int level = 0;
		String line = null;
		
		try (
				BufferedReader reader = new BufferedReader(new FileReader(inputPath));
				BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath));) {
			
			while (true) {
				line = reader.readLine();
				
				// A little feedback
				lines++;
				if (lines % 1000000 == 0) {
					System.out.println(lines);
				}
				
				if (line == null) {
					break;
				}
				
				level += countMatches(line, "{");
				if (!line.trim().equals("")) {
					curRec.append(line);
				}
				level -= countMatches(line, "}");
				if (level == 0) {
					writer.write(curRec.toString());
					writer.write('\n');
					curRec.delete(0, curRec.length());
				}
			}
			
			reader.close();
			writer.close();
			
			long end = System.currentTimeMillis();
			long elapsed = end - start;
			System.out.println("Total time in seconds: " + elapsed / 1000);
		}
	}
	
	public static int countMatches(String str, String sub) {
		if (isEmpty(str) || isEmpty(sub)) {
			return 0;
		}
		int count = 0;
		int idx = 0;
		while ((idx = str.indexOf(sub, idx)) != INDEX_NOT_FOUND) {
			count++;
			idx += sub.length();
		}
		return count;
	}
	
	public static boolean isEmpty(String str) {
		return str == null || str.length() == 0;
	}
	
}
