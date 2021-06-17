package com.adacho.driver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.adacho.mapper.DelayCountMapperWithMultipleOutputs;
import com.adacho.reducer.DelayCountReducerWithMultipleOutputs;

public class DelayCountWithMultipleOutputs extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DelayCountWithMultipleOutputs(), args);
		System.out.println("MR-Job Result: " + res);
	}
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.out.println("Usage: DelayCountWithMultipleOutputs <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(getConf(), "DelayCountWithMultipleOutputs");
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.setJarByClass(DelayCountWithMultipleOutputs.class);
		job.setMapperClass(DelayCountMapperWithMultipleOutputs.class);
		job.setReducerClass(DelayCountReducerWithMultipleOutputs.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		MultipleOutputs.addNamedOutput(job, "departure", TextOutputFormat.class, 
	    		Text.class, IntWritable.class);
	    MultipleOutputs.addNamedOutput(job, "arrival", TextOutputFormat.class, 
    		Text.class, IntWritable.class);
		job.waitForCompletion(true);
		return 0;
	}
}