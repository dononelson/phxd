package com.visa.json.parser;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class IOCSimpleParser extends Configured implements Tool {
	
	private Configuration conf;
	static Logger log = Logger.getLogger("mapred.audit.logger");
	
	public IOCSimpleParser() {
		super();
		setConf(getConf());
		System.out.println("Conf = " + this.conf);
	}
	
	public static void main(String[] argv) throws Exception {
		if (argv.length < 2) {
			System.err.println("Usage: IOCSimpleParser <inputPath> <outputPath");
			System.exit(1);
		}
		
		int result = ToolRunner.run(new IOCSimpleParser(), argv);
		
		System.exit(result);
	}
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: IOCJacksonParser <inputPath> <outputPath");
			System.exit(1);
		}
		
		Job job = null;
		try {
			job = Job.getInstance(conf, "JSON Parsing");
		}
		catch (IOException e) {
			e.printStackTrace();
			log.fatal(e.getLocalizedMessage());
		}
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(IOCSimpleParser.SimpleMapper.class);
		job.setNumReduceTasks(0); // No reducer needed for this job
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		// This class is in the jar file that will be executed on each node for MR tasks
		job.setJarByClass(IOCSimpleParser.class);
		
		try {
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		}
		catch (IOException e) {
			e.printStackTrace();
			log.fatal(e.getLocalizedMessage());
		}
		
		int success = 0;
		try {
			success = job.waitForCompletion(true) ? 1 : 0;
		}
		catch (ClassNotFoundException | IOException | InterruptedException e) {
			e.printStackTrace();
			log.error(e);
		}
		return success;
	}
	
	@Override
	public void setConf(Configuration con) {
		this.conf = con;
	}
	
	@Override
	public Configuration getConf() {
		return this.conf;
	}
	
	/**
	 * The mapper class for this MR job. It takes a JSON record and writes it to the output location delimited by "|".
	 * The constraint is that the JSON record is on one "line" (an entire record is passed in by the caller).
	 */
	// TODO - parameterize the constants and fields in this class.
	// TODO parameterize DELIM
	// TODO parameterize removeLastDelim
	
	public static class SimpleMapper extends Mapper<Object, Text, Text, Text> {
		
		private static final char DELIM = '|';
		private static final String MARK = "<<<<<<<<<<<==============";
		
		private JSONParser parser = new JSONParser();
		private StringBuilder parsed = new StringBuilder();
		private String inStr = null;
		
		ContainerFactory cf = new ContainerFactory() {
			
			@Override
			public List<String> creatArrayContainer() {
				return new LinkedList<String>();
			}
			
			@Override
			public Map<String, String> createObjectContainer() {
				return new LinkedHashMap<String, String>();
			}
		};
		
		// Set this to true to remove last DELIM character if last field is blank
		private boolean removeLastDelim = false;
		
		@Override
		public void map(Object key, Text value, Context context) {
			inStr = value.toString();
			
			try {
				if (inStr != null && !inStr.equals("")) {
					String tmp = parse(inStr);
					flush(tmp, context);
					return;
				}
			}
			catch (ParseException e1) {
				log.error(MARK + " Parse exception");
				log.error(MARK + e1.getLocalizedMessage());
				log.error(MARK + "String in question: " + inStr);
			}
			catch (IOException e) {
				log.error(MARK + " IO exception");
				log.error(MARK + e.getLocalizedMessage());
			}
			catch (InterruptedException e) {
				log.error(MARK + " Interrupted exception");
				log.error(MARK + e.getLocalizedMessage());
			}
			
		}
		
		@SuppressWarnings({ "unchecked" })
		private String parse(String aString) throws ParseException {
			
			boolean first = true;
			clearStrBuilderj(parsed);
			Map<String, String> map = ((Map<String, String>) parser.parse(aString, cf));
			Iterator<Map.Entry<String, String>> it = map.entrySet().iterator();
			
			while (it.hasNext()) {
				Map.Entry<String, String> entry = it.next();
				String value = entry.getValue().toString();
				if (first) {
					first = false;
				}
				else {
					parsed.append(DELIM);
				}
				parsed.append(value);
			}
			
			// Optionally remove last delimiter if last field is blank
			if (removeLastDelim) {
				if (parsed.charAt(parsed.length() - 1) == DELIM) {
					parsed.deleteCharAt(parsed.length() - 1);
				}
			}
			return parsed.toString();
		}
		
		private void flush(String txt, Context context) throws IOException, InterruptedException {
			context.write(new Text(txt), null);
		}
		
		private void clearStrBuilderj(StringBuilder toClear) {
			
			if (toClear != null && toClear.length() > 0) {
				toClear.delete(0, toClear.length());
			}
		}
	}
}
