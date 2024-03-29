package com.visa.json.parser;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper.Context;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

// @Ignore
public class JacksonMapperTests {
	
	IOCSimpleMapper mapper;
	BufferedReader reader = null;
	Context context = null;
	LongWritable key = new LongWritable(0);
	
	public JacksonMapperTests() {
	}
	
	@Before
	public void testInit() throws FileNotFoundException {
		reader = new BufferedReader(new FileReader("/tmp/sample.json"));
		mapper = new IOCSimpleMapper();
	}
	
	@After
	public void testCleanup() throws IOException {
		reader.close();
	}
	
	@Test
	public void testParseJSONLine() throws IOException {
		Text text = null;
		String inText = null;
		do {
			inText = reader.readLine();
			text = new Text(inText);
			if (text != null) {
				mapper.map(key, text, context);
			}
		}
		while (text != null);
		
	}
}
