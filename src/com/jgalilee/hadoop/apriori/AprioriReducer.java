package com.jgalilee.hadoop.apriori;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class AprioriReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

	protected int minSup = -1;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration config = context.getConfiguration();
		this.minSup = config.getInt("apriori.iterations.job", -1);
	}
	
	public void reduce(Text outKey, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
			if (sum >= minSup) {
				context.getCounter(AprioriCounters.NewRules).increment(1);
				context.write(outKey, NullWritable.get());
				break;
			}
		}
		return;
	}
	
}
