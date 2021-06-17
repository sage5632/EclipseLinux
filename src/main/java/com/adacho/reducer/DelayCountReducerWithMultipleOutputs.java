package com.adacho.reducer;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
public class DelayCountReducerWithMultipleOutputs 
	extends Reducer<Text, IntWritable, Text, IntWritable>{
	private MultipleOutputs<Text, IntWritable> mos;
	private Text outputKey = new Text();
	private IntWritable result = new IntWritable();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] columns = key.toString().split(",");
		outputKey.set(columns[1] + "," + columns[2]);
		int sum = 0;
		for(IntWritable value : values) {
			sum += value.get();
		}
		result.set(sum);
		if(columns[0].equals("D"))
			mos.write("depature", outputKey, result);
		else
			mos.write("arrival", outputKey, result);
	}
	@Override
	protected void setup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		mos = new MultipleOutputs<Text, IntWritable>(context);
	}
}