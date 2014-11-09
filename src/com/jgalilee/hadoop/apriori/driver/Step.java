package com.jgalilee.hadoop.apriori.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.jgalilee.hadoop.apriori.AprioriCombiner;
import com.jgalilee.hadoop.apriori.AprioriMapper;
import com.jgalilee.hadoop.apriori.AprioriReducer;
import com.jgalilee.java.patterns.resurecting.Iteration;

/**
 * Apriori Hadoop algorithm step job following the Iteration interface.
 *
 * @author jgalilee
 */
public class Step extends Job implements Iteration {

	@SuppressWarnings("deprecation")
	public Step(Configuration conf) throws IOException {
		super(conf);
		setJarByClass(getClass());
		addCacheFiles();
		FileOutputFormat.setOutputPath(this, getOutputPath(conf.getInt("iteration", 0)));
		FileInputFormat.addInputPath(this, getInputPath());
		setMapperClass(AprioriMapper.class);
		setCombinerClass(AprioriCombiner.class);
		setReducerClass(AprioriReducer.class);
		setMapOutputKeyClass(Text.class);
		setMapOutputValueClass(IntWritable.class);
		setOutputKeyClass(Text.class);
		setOutputValueClass(NullWritable.class);
		setNumReduceTasks(conf.getInt("number", getNumReduceTasks()));
	}

	@Override
	public Boolean run() {
		int iteration = conf.getInt("iteration", 0);
		if (iteration > 3) {
			Path output = getOutputPath(iteration - 2);
			try {
				delete(output);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		Path output = getOutputPath(iteration);
		try {
			FileSystem dfs = FileSystem.get(getConfiguration());
			if (dfs.isDirectory(output)) {
				dfs.delete(output, true);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		Boolean result = false;
		try {
			result = waitForCompletion(true);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Delete the given file from the distributed file system.
	 */
	private void delete(Path output) throws IOException {
		FileSystem fs = FileSystem.get(this.conf);
		fs.delete(output, true);
	}

	@Override
	public Boolean retry() {
		return run();
	}

	/**
	 * Return the path to the iteration step's output.
	 *
	 * @return Return the path to the iteration step's output.
	 */
	private Path getOutputPath(int iteration) {
		String sep = System.getProperty("file.separator");
		System.out.println("Using output " + conf.get("output") + sep + String.valueOf(iteration));
		return new Path(conf.get("output") + sep + String.valueOf(iteration));
	}

	/**
	 * Return the path to the iteration step's input.
	 *
	 * @return Return the path to the iteration step's input.
	 */
	private Path getInputPath() {
		System.out.println("Using input " + conf.get("input"));
		return new Path(conf.get("input"));
	}

	/**
	 * Adds the files from the previous iteration to the distributed cache.
	 */
	private void addCacheFiles() throws IOException {
		Integer iteration = conf.getInt("iteration", 0);
		if (iteration > 0) {
			String sep = System.getProperty("file.separator");
			String output = conf.get("output") + sep + String.valueOf(conf.getInt("iteration", 0) - 1);
			Path out = new Path(output, "part-r-[0-9]*");
			System.out.println("Checking path " + out.toString());
			FileSystem fs = FileSystem.get(conf);
			FileStatus[] ls = fs.globStatus(out);
			for (FileStatus fileStatus : ls) {
				Path pfs = fileStatus.getPath();
				System.out.println("Adding " + pfs.toUri().toString());
				addCacheFile(pfs.toUri());
			}
		}
	}

}
