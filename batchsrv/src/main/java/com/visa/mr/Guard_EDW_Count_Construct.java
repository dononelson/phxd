package com.visa.mr;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class Guard_EDW_Count_Construct {
	public static void main(String[] args) throws Exception {

		JobConf conf = new JobConf(Guard_EDW_Count_Construct.class);
		// Configuration conf = new Configuration();
		conf.setJobName("Guard_EDW_Count_Construct");
		// conf.setJar("/data1/gisdata/Guardium_DB_Audit/MR_Guard_Audit/Guard_EDW_Count_Construct.jar");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(NullWritable.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		conf.setMapperClass(Guard_EDW_Count_Construct_Mapper.class);
		// conf.setCombinerClass(Guard_EDW_DCL_Reducer.class);
		// conf.setReducerClass(Guard_EDW_DCL_Reducer.class);
		conf.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}
