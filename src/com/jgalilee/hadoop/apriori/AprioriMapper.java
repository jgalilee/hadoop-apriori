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

import com.jgalilee.algorithms.CombinationChecker;
import com.jgalilee.algorithms.CombinationGenerator;
import com.jgalilee.algorithms.CombinatorialException;

/**
 * 
 * @author jgalilee
 *
 */
public class AprioriMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final String RECORD_DELEMITER = "	";
	private final String ITEM_DELIMITER = " ";	
	
	protected int k = -1;
	protected CombinationChecker candidateChecker = new CombinationChecker(ITEM_DELIMITER);
	
	protected Text candidate = new Text();
	protected IntWritable one = new IntWritable(1);
	
	/**
	 * 
	 * @param context
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		Configuration config = context.getConfiguration();
		k = config.getInt("apriori.iterations.current", 1);
		if (k > 1) {
			List<URI> uris = Arrays.asList(context.getCacheFiles());
			for (URI uri : uris) {
				Path p = new Path(uri);
				FileSystem fs = FileSystem.get(context.getConfiguration());
				InputStreamReader ir = new InputStreamReader(fs.open(p));
				BufferedReader br = new BufferedReader(ir);
				String line;
				line = br.readLine();
				while (line != null){
					candidateChecker.add(line);
					line = br.readLine();
				}
			}
		}
		return;
	}
	
	/**
	 * 
	 * @param items
	 * @return
	 * @throws Exception 
	 */
	public boolean isFrequent(int k, List<String> items) throws CombinatorialException {
		CombinationGenerator<String> candidateGenerator = new CombinationGenerator<String>();
		if (k > 0) {
			candidateGenerator.reset(k, items);
			while (candidateGenerator.hasNext()) {
				String itemset = candidateGenerator.next();
				if (!candidateChecker.valid(itemset)) {
					return false;
				}
			}
			
		}
		return true;
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		CombinationGenerator<String> candidateGenerator = new CombinationGenerator<String>();
		List<String> items = getItemsFromRecord(value.toString());
		if (null != items) {
			candidateGenerator.reset(k, items);
			while (candidateGenerator.hasNext()) {
				String itemset = candidateGenerator.next();
				try {
					if (isFrequent(k-1, itemset)) {
						candidate.set(getRecordFromItems(itemset));
						context.write(candidate, one);
					}
				} catch (CombinatorialException e) {
					System.err.println(e.getMessage());
					e.printStackTrace(System.err);
				}
			}
		
		}
		return;
	}

	/**
	 * 
	 * @param tuple
	 * @return
	 */
	public List<String> getItemsFromRecord(String tuple) {
		String[] columns = tuple.split(RECORD_DELEMITER);
		if (columns.length > 1) {
			String[] items = columns[1].split(ITEM_DELIMITER);
			if (items.length > 0) {
				Arrays.sort(items);
				return Arrays.asList(items);
			}
		}
		return null;
	}
	
	/**
	 * 
	 * @param items
	 * @return
	 */
	public String getRecordFromItems(List<String> items) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < items.size(); i ++) {
			sb.append(items.get(i));
			if (i < items.size() - 1) {
				sb.append(ITEM_DELIMITER);
			}
		}
		return sb.toString();
	}
	
}
