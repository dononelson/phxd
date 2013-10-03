package com.visa.mr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GuardEDWCountMapper extends Mapper {

	public GuardEDWCountMapper() {
	}

	private final static NullWritable nw = NullWritable.get();
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if (!line.contains("SQL Verb")) {
			String line22 = line.replaceAll("\",", "|");
			String line2 = line22.replaceAll("\"", "");
			String line3 = null;

			char[] line_arr = line2.toCharArray();

			int len = line2.length();
			int delim_count = 0;
			// int replace_count = 0;
			for (int ii = 0; ii < len; ii++) {
				if (line_arr[ii] == '|') {
					delim_count++;
				}
				if ((line_arr[ii] == ' ')
						&& ((delim_count == 1) || (delim_count == 2))) {
					line_arr[ii] = '|';
				}
			}
			line3 = new String(line_arr);
			line3.trim();

			int counts = 0;
			int idx = 0;
			String sub = "|";
			while ((idx = line3.indexOf(sub, idx)) != -1) {
				counts++;
				idx += sub.length();
			}
			if (counts == 8) // //there are 9 columns. so 8 pipe characters in
								// each record
			{
				word.set(line3);
				// output.collect(word, nw);
				context.write(null, word);
			}
		}
	}
}
