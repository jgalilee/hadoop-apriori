package com.jgalilee.hadoop.apriori.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.jgalilee.java.patterns.resurecting.Iteration;
import com.jgalilee.java.patterns.resurecting.IterativeAlgorithm;

/**
 * Apriori Hadoop driver. Uses the resurrecting iterator interface to easily
 * manage the iterative nature of the algorithms execution.
 *
 * @author jgalilee
 */
public class Driver extends Configured implements Tool {

	private static final String USAGE = "USAGE %s: home input output minsup max number\n";

	@Override
	public int run(String[] args) throws Exception {
		if (args.length < 5) {
			System.err.printf(USAGE, getClass());
			ToolRunner.printGenericCommandUsage(System.err);
			return 1;
		}
		Configuration conf = getConf();
		conf.set("home", String.valueOf(args[0]));
		conf.set("input", String.valueOf(args[1]));
		conf.set("output", String.valueOf(args[2]));
		conf.setLong("minsup", Long.valueOf(args[3]));
		conf.setInt("max", Integer.valueOf(args[4]));
		conf.setInt("number", Integer.valueOf(args[5]));
		// Create an instance of the iterative algorithm and configure it.
		IterativeAlgorithm apriori = new Algorithm(conf);
		apriori.resurrect();
		while(!apriori.converged()) {
			Iteration step = apriori.step();
			System.out.printf("Starting Iteration %s\n", apriori.getIteration());
			boolean result = step.run();
			if (!result && !step.retry()) {
				throw new Exception("Iteration failed!");
			}
			if (!apriori.commit()) {
				throw new Exception("Unable to commit iteration state!");
			}
		}
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Driver(), args);
		System.exit(exitCode);
	}

}
