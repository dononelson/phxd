package com.visa.json.parser;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class IOCSimpleMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static final char DELIM = '|';
	private static final String START_OBJ = "{";
	private static final String END_OBJ = "}";
	private static final String MARK = "<<<<<<<<<<<==============";
	
	private static StringBuilder jsonObject = new StringBuilder();
	// private static StringBuilder parsed = new StringBuilder();
	private static int braceLevel = 0;
	private JSONParser parser = new JSONParser();
	private final Logger log;
	private ContainerFactory cf = null;
	private StringBuilder parsed = null;
	private static boolean tryParseImmediate = true;
	private int counter = 0;
	
	public IOCSimpleMapper() {
		System.out.println("In IOCSimpleMapper");
		log = Logger.getLogger(getClass());
		parsed = new StringBuilder();
		
		cf = new ContainerFactory() {
			
			@Override
			public List<String> creatArrayContainer() {
				return new LinkedList<String>();
			}
			
			@Override
			public Map<String, String> createObjectContainer() {
				return new LinkedHashMap<String, String>();
			}
		};
		
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) {
		if (counter > 10) {
			return;
		}
		String inStr = value.toString();
		//		log.info(MARK + " In IOCSimpleMapper.map()");
		
		if (tryParseImmediate) {
			// First, try to parse the in value just as is, in case it's not
			// multi-line
			try {
				log.debug(MARK + "About to parse immediate: " + inStr);
				if (inStr != null && !inStr.equals("")) {
					String tmp = parse(inStr);
					//					log.info(MARK + " Successfully parsed record");
					System.out.println(MARK + " Parsed " + inStr);
					flush(tmp, context);
					clearJson();
					return;
				}
			}
			catch (ParseException e1) {
				log.error(MARK + " Parse exception");
				e1.printStackTrace();
			}
			catch (IOException e) {
				log.error(MARK + " IO exception");
				e.printStackTrace();
			}
			catch (InterruptedException e) {
				log.error(MARK + " Interrupted exception");
				e.printStackTrace();
			}
		}
		
		tryParseImmediate = false;
		braceLevel += StringUtils.countMatches(inStr, START_OBJ);
		//		log.debug(MARK + " braceLevel = " + braceLevel);
		jsonObject.append(inStr);
		braceLevel -= StringUtils.countMatches(inStr, END_OBJ);
		//		log.debug(MARK + " braceLevel = " + braceLevel);
		
		if (braceLevel == 0) {
			try {
				String parsedJson = parse(jsonObject.toString());
				context.write(null, new Text(parsedJson));
			}
			catch (IOException | InterruptedException | ParseException e) {
				e.printStackTrace();
				clearJson();
				tryParseImmediate = true;
			}
			clearJson();
		}
		else if (braceLevel < 0) {
			// Error in processing, partial file, etc.
			// clear the contents - bad data
			clearJson();
			braceLevel = 0;
			tryParseImmediate = true;
		}
		
	}
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private String parse(String inStr) throws ParseException {
		System.out.println("In IOCSimpleMapper.parse");
		++counter;
		boolean first = true;
		clearStrBuilderj(parsed);
		Map<String, String> map = (Map<String, String>) parser.parse(inStr, cf);
		Iterator it = map.entrySet().iterator();
		
		while (it.hasNext()) {
			Map.Entry entry = (Map.Entry) it.next();
			String value = entry.getValue().toString();
			if (first) {
				first = false;
			}
			else {
				parsed.append(DELIM);
			}
			parsed.append(value);
		}
		
		if (parsed.charAt(parsed.length() - 1) == DELIM) {
			parsed.deleteCharAt(parsed.length() - 1);
		}
		return parsed.toString();
	}
	
	private void flush(String txt, Context context) throws IOException, InterruptedException {
		context.write(new Text(txt), null);
		
	}
	
	private void clearJson() {
		clearStrBuilderj(jsonObject);
	}
	
	private void clearStrBuilderj(StringBuilder toClear) {
		if (toClear.length() > 0) {
			toClear.delete(0, toClear.length());
		}
	}
}
