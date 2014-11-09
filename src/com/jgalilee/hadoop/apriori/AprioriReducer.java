package com.jgalilee.hadoop.apriori;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Counts the number of new rules generated and writes the rules with a sum that
 * is greater than the minimum support. This is calculated lazily so the first
 * time the item meets the minimum support it is written.
 *
 * @author jgalilee
 */
public class AprioriReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

	private Counter newRules;
	private int minSup = -1;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		newRules = context.getCounter(AprioriCounters.NEW_RULES);
		Configuration config = context.getConfiguration();
		this.minSup = config.getInt("minsup", -1);
	}

	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable value : values) {
			sum += value.get();
			if (sum >= minSup) {
				newRules.increment(1L);
				context.write(key, NullWritable.get());
				break;
			}
		}
		return;
	}

}
