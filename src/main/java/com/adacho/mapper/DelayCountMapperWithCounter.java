package com.adacho.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.adacho.common.AirlinePerformanceParser;
import com.adacho.counter.DelayCounters;

public class DelayCountMapperWithCounter extends Mapper<LongWritable, Text, Text, IntWritable> {
	private String workType;
	private final static IntWritable outputValue = new IntWritable(1);
	private Text outputKey = new Text();
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		workType = context.getConfiguration().get("workType");
	}
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
		
		outputKey.set(parser.getYear() + "," + parser.getMonth());
		
		if(workType.equals("departure")) {
			if(parser.isDepartureDelayAvailable()) {
				if (parser.getDepartureDelayTime() > 0) {
				outputKey.set(parser.getYear() + "," + parser.getMonth());
				context.write(outputKey, outputValue);
			} else if(parser.getDepartureDelayTime() == 0) {
				context.getCounter(DelayCounters.scheduled_departure).increment(1);
			}
		}
		} else if(parser.isArriveDelayAvailable()) {
			if (parser.getArriveDelayTime() > 0) {
			outputKey.set(parser.getYear() + "," + parser.getMonth());
			context.write(outputKey, outputValue);
		} else if(parser.getArriveDelayTime() == 0) {
			context.getCounter(DelayCounters.scheduled_departure).increment(1);
		}
		}
	}
	}

