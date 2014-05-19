package com.jgalilee.hadoop.apriori;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * @author jgalilee
 *
 */
public class AprioriDriver extends Configured implements Tool {

	private static String NAME_ITERATION_OPT = "apriori.iterations.job";
	private static String CURRENT_ITERATION_OPT = "apriori.iterations.current";
	private static String LAST_ITERATION_OPT = "apriori.iterations.last";
	private static String MAX_ITERATIONS_OPT = "apriori.iterations.max";
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.printf("Usage: %s [options] <in> <out>\n", getClass());
			ToolRunner.printGenericCommandUsage(System.err);
			return 1;
		}
		
		Configuration conf = getConf();
		String input= args[0];
		String outputRoot = args[1];
		String output = null;
		
		long rules = 1L;
		int iteration = 0;
		int maxIterations = conf.getInt(MAX_ITERATIONS_OPT, -1);
		while(iteration++ < maxIterations && rules > 0) {
			
			conf.set(CURRENT_ITERATION_OPT, String.valueOf(iteration));
			conf.set(LAST_ITERATION_OPT, String.valueOf(output));
			
			Job job = Job.getInstance(conf, NAME_ITERATION_OPT + iteration);
			job.setJarByClass(getClass());
			
			if (null != output) {
				Path out = new Path(output, "part-r-[0-9]*");
				FileSystem fs = FileSystem.get(conf);
				FileStatus[] ls = fs.globStatus(out);
				for (FileStatus fileStatus : ls) {
					Path pfs = fileStatus.getPath();
					job.addCacheFile(pfs.toUri());
				}
			}
			
			output = outputRoot + "/" + iteration;
			FileInputFormat.addInputPath(job, new Path(input));
			FileOutputFormat.setOutputPath(job, new Path(output));
			
			job.setMapperClass(AprioriMapper.class);
			job.setCombinerClass(AprioriCombiner.class);
			job.setReducerClass(AprioriReducer.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			job.setNumReduceTasks(3);
			job.waitForCompletion(true);
			
			Counters counters = job.getCounters();
			rules = counters.findCounter(AprioriCounters.NewRules).getValue();
		}
		return 0;
	}
	
	/**
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new AprioriDriver(), args);
		System.exit(exitCode);
	}
}
