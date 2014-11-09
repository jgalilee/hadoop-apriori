package com.jgalilee.hadoop.apriori;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.jgalilee.hadoop.apriori.util.CombinationChecker;
import com.jgalilee.hadoop.apriori.util.CombinationGenerator;

/**
 * Outputs all of the candidate frequent itemset sets of size k with an
 * initial count of one.
 *
 * @author jgalilee
 */
public class AprioriMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	protected int k = -1;
	protected Text candidate = new Text();
	protected IntWritable one = new IntWritable(1);
	protected CombinationChecker candidateChecker = new CombinationChecker();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration config = context.getConfiguration();
		k = config.getInt("iteration", 1);
		if (k > 1) {
			List<URI> uris = Arrays.asList(context.getCacheFiles());
			for (URI uri : uris) {
				Path p = new Path(uri);
				System.out.println("Loading " + uri.toString());
				FileSystem fs = FileSystem.get(context.getConfiguration());
				InputStreamReader ir = new InputStreamReader(fs.open(p));
				BufferedReader br = new BufferedReader(ir);
				String line;
				line = br.readLine();
				while (line != null) {
					candidateChecker.add(line);
					line = br.readLine();
				}
			}
		}
		return;
	}

	public boolean isFrequent(int k, int[] items) {
		CombinationGenerator candidateGenerator = new CombinationGenerator();
		if (k > 0) {
			candidateGenerator.reset(k, items);
			while (candidateGenerator.hasNext()) {
				if (!candidateChecker.valid(candidateGenerator.next())) {
					return false;
				}
			}
		}
		return true;
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		CombinationGenerator candidateGenerator = new CombinationGenerator();
		int[] items = candidateChecker.getItemsFromRecord(value.toString());
		if (!Arrays.equals(CombinationGenerator.BLANK_COMBINATION, items)) {
			candidateGenerator.reset(k, items);
			while (candidateGenerator.hasNext()) {
				int[] itemset = candidateGenerator.next();
				if (isFrequent(k-1, itemset)) {
					candidate.set(Arrays.toString(itemset));
					context.write(candidate, one);
				}
			}
		}
		return;
	}

}
