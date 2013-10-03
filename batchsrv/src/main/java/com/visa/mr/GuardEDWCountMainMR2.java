package com.visa.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class GuardEDWCountMainMR2 extends Configured implements Tool {

	@Override
	public int run(String[] arg) throws Exception {
		Job job = Job.getInstance(new Configuration());

		job.setJobName("GuardEDW Count");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(GuardEDWCountMapper.class);
		// job.setReducerClass(Reduce.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path(arg[0]));
		FileOutputFormat.setOutputPath(job, new Path(arg[1]));

		job.setJarByClass(GuardEDWCountMainMR2.class);

		job.submit();

		return 0;
	}

	public static void main(String[] args) {
		if (args.length != 2) {
			System.err
					.println("Usage: GuardEDWCountMainMR2 input_dir output_dir");
			System.exit(1);
		}
		GuardEDWCountMainMR2 main = new GuardEDWCountMainMR2();
		try {
			main.run(args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
