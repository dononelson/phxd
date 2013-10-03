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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

@SuppressWarnings("rawtypes")
public class IOCParser extends Configured implements Tool {
	private Configuration conf;

	public static final String INPUT = "input";
	public static final String OUTPUT = "output";
	public static final String DT_KEY = "devTimeISO";

	public void setConf(Configuration conf) {
		this.conf = conf;
	}

	public Configuration getConf() {
		return conf;
	}

	static class IOCParserMapper extends Mapper<Object, Text, Text, Text> {
		JSONParser parser = new JSONParser();
		Map.Entry entry = null;
		Map jsonMap = null;
		Iterator it = null;
		String inLine = null, tmp = null, outLine;

		ContainerFactory cf = new ContainerFactory() {

			public List<?> creatArrayContainer() {
				return new LinkedList();
			}

			public Map createObjectContainer() {
				return new LinkedHashMap();
			}

		};

		private String parseJson(String srcLine) throws ParseException {
			outLine = "";
			int fc = 0;

			System.out.println("Source line = " + srcLine);

			jsonMap = (Map) parser.parse(srcLine, cf);
			it = jsonMap.entrySet().iterator();

			while (it.hasNext()) {
				entry = (Map.Entry) it.next();
				tmp = entry.getValue().toString();
				System.out.println(tmp);
				tmp = tmp.replaceAll("\\r\\n", "").replaceAll("\\r", "")
						.replaceAll("\\n", "").replaceAll("\n", "")
						.replaceAll("\\t", "").replaceAll("^M", "");
				tmp = tmp.replaceAll("[^\\p{Print}]", "");

				if (entry.getKey().toString().equalsIgnoreCase(DT_KEY)) {
					tmp = tmp.replaceAll("T", ":").replaceAll("Z", "");
				}

				outLine = outLine + ((fc > 0) ? "\t" : "") + tmp;
				fc++;

			}

			System.out.println("Outline = " + outLine);

			return outLine;
		}

		@Override
		public void map(Object key, Text value, Context context)
				throws IOException {

			try {
				inLine = value.toString();
				outLine = parseJson(inLine);
				context.write(new Text(key.toString()), new Text(outLine));

			} catch (Exception e) {
				System.out.println("Error:" + e.toString() + "\n"
						+ e.getStackTrace());
			}
		}

	}// mapper

	public static void main(String[] argv) throws Exception {
		if (argv.length < 2) {
			System.err.println("Usage: IOCParser <inputPath> <outputPath");
			System.exit(1);
		}
		ToolRunner.run(new IOCParser(new Configuration()), argv);
	}

	public IOCParser(Configuration conf) {
		// FIXME Hack
		this.conf = super.getConf();
	}

	@Override
	@SuppressWarnings("deprecation")
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: IOCParser <inputPath> <outputPath");
			System.exit(1);
		}
		for (int i = 0; i < args.length; i++) {
			System.out.println("Arg " + i + " = " + args[i]);
		}

		Job job = new Job(conf, "IOCParser");

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(IOCParserMapper.class);
		job.setNumReduceTasks(0);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(IOCParser.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 1 : 0;

	}// run
}
