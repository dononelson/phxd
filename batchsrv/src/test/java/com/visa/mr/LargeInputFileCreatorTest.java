package com.visa.mr;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.junit.Ignore;

@Ignore
public class LargeInputFileCreatorTest {
	
	private static BufferedWriter writer;
	private static BufferedReader reader;
	String line = null;
	StringBuffer jsonText = new StringBuffer();
	//	int MAX_FILE_SIZE = Integer.MAX_VALUE - 50000;
	private static final int MAX_FILE_SIZE = 5000;
	
	@Ignore
	public void create2GBJsonMutliLineFile() throws IOException {
		// If you really want to run this, have 2GB available in the /tmp directory
		// And you have to have a sample multi-line JSON file named "sample.json"
		// in the /tmp directory
		
		reader = new BufferedReader(new FileReader("/tmp/sample.json"));
		writer = new BufferedWriter(new FileWriter("/tmp/huge.json"));
		
		List<String> origLines = new ArrayList<String>();
		
		while (true) {
			try {
				line = reader.readLine();
				origLines.add(line);
			}
			catch (IOException e) {
				e.printStackTrace();
				System.exit(1);
			}
			if (line == null) {
				break;
			}
		}
		reader.close();
		
		int newFileSize = 0;
		while (newFileSize <= MAX_FILE_SIZE) {
			for (int i = 0; i < origLines.size(); i++) {
				String x = origLines.get(i);
				if (x != null) {
					newFileSize += x.length();
					writer.write(x);
					writer.write('\n');
					newFileSize++;
				}
				else {
					break;
				}
			}
		}
		writer.close();
		
	}
	
	@Ignore
	public void stripLFTest() throws IOException {
		long start = System.currentTimeMillis();
		reader = new BufferedReader(new FileReader("/tmp/huge.json"));
		writer = new BufferedWriter(new FileWriter("/tmp/huge_stripped.json"));
		StringBuffer curRec = new StringBuffer();
		int level = 0;
		//		int counter = 0;
		
		while (true) {
			line = reader.readLine();
			
			if (line == null) {
				break;
			}
			
			level += StringUtils.countMatches(line, "{");
			if (!line.trim().equals("")) {
				curRec.append(line);
			}
			level -= StringUtils.countMatches(line, "}");
			if (level == 0) {
				writer.write(curRec.toString());
				writer.write('\n');
				//				curRec = new StringBuffer();
				curRec.delete(0, curRec.length());
				//				++counter;
			}
			//			if (counter > 5) {
			//				break;
			//			}
		}
		reader.close();
		writer.close();
		long end = System.currentTimeMillis();
		long elapsed = end - start;
		System.out.println("Total time in seconds: " + elapsed / 1000);
	}
}
