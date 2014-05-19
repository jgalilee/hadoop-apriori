package com.jgalilee.hadoop.apriori;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AprioriCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable value = new IntWritable();
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
		}
		value.set(sum);
		context.write(key, value);
		return;
	}
	
}
