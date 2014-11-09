package com.jgalilee.hadoop.apriori.driver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;

import com.jgalilee.hadoop.apriori.AprioriCounters;
import com.jgalilee.hadoop.apriori.driver.Step;
import com.jgalilee.java.patterns.resurecting.Iteration;
import com.jgalilee.java.patterns.resurecting.IterativeAlgorithm;

/**
 * Apriori Hadoop algorithm driver following the IterativeAlgorithm interface.
 *
 * @author jgalilee
 */
public class Algorithm implements IterativeAlgorithm {

	private Integer iteration = 0;
	private Step previousStep = null;
	private Step currentStep = null;
	private Configuration conf = null;

	/**
	 * Constructs the Apriori algorithm instance from the given configuration.
	 */
	public Algorithm(Configuration conf) {
		this.conf = conf;
		this.iteration = this.conf.getInt("iteration", 0);
	}

	@Override
	public Iteration step() {
		try {
			iteration++;
			conf.setInt("iteration", iteration);
			Step tempStep = new Step(conf);
			previousStep = currentStep;
			currentStep = tempStep;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return currentStep;
	}

	@Override
	public Boolean converged() {
		Boolean result = false;
		if (null != currentStep) {
			Integer max = conf.getInt("max", 0);
			if (getIteration() < max) {
				Counters counters;
				try {
					counters = currentStep.getCounters();
					Enum<?> convergenceCounter = AprioriCounters.NEW_RULES;
					Counter counter = counters.findCounter(convergenceCounter);
					result = (0L == counter.getValue());
					System.out.printf("Algorithm counter %s == %s\n", 0, counter.getValue());
				} catch (IOException e) {
					e.printStackTrace();
				}
			} else {
				System.out.printf("Algorithm converged max iterations\n");
				result = true;
			}
		}
		return result;
	}

	@Override
	public Boolean commit() {
		try{
			Path homePath = new Path(conf.get("home"));
			FileSystem dfs = FileSystem.get(new Configuration());
			if (!dfs.isFile(homePath)) {
				dfs.createNewFile(homePath);
			}
			FSDataOutputStream dfsOutput = dfs.create(homePath);
			conf.write(dfsOutput);
			return true;
		} catch(Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public Boolean resurrect() {
		try{
			Path homePath = new Path(conf.get("home"));
			FileSystem dfs = FileSystem.get(new Configuration());
			if (dfs.isFile(homePath)) {
				FSDataInputStream dfsInput = dfs.open(homePath);
				conf.readFields(dfsInput);
			}
			return true;
		} catch(Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public Integer getIteration() {
		return iteration;
	}

	/**
	 * Get the last iteration step for the algorithm.
	 *
	 * @return Last iteration step for the algorithm.
	 */
	public Step previousStep() {
		return previousStep;
	}

	/**
	 * Get the current iteration step for the algorithm.
	 *
	 * @return Current iteration step for the algorithm.
	 */
	public Step currentStep() {
		return currentStep;
	}

}
