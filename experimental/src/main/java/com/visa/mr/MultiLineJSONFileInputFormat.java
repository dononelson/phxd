package com.visa.mr;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

public class MultiLineJSONFileInputFormat extends FileInputFormat {
	
	private static final Logger log = Logger.getLogger(MultiLineJSONFileInputFormat.class);
	
	public MultiLineJSONFileInputFormat() {
	}
	
	@Override
	public RecordReader createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		log.error("zxqzxqzxq Creating new JSONRecordLineReader");
		return new JSONLineRecordReader();
	}
	
}
