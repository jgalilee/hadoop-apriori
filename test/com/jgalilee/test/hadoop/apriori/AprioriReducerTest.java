package com.jgalilee.test.hadoop.apriori;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import com.jgalilee.hadoop.apriori.AprioriReducer;

public class AprioriReducerTest {

	private ReduceDriver<Text, IntWritable, Text, NullWritable> reduceDriver;

	@Before
	public void setUp() {
		AprioriReducer reducer = new AprioriReducer();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, NullWritable>();
		reduceDriver.setReducer(reducer);
		Configuration conf = reduceDriver.getConfiguration();
		conf.set("minsup", "3");
	}
	
	@Test
	public void testReduceSupported() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(4));
		values.add(new IntWritable(5));
		reduceDriver.withInput(new Text("A"), values);
		reduceDriver.withOutput(new Text("A"), NullWritable.get());
		reduceDriver.runTest();
	}
	
	@Test
	public void testReduceUnsupported() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("B"), values);
		reduceDriver.withAll(new ArrayList<Pair<Text,List<IntWritable>>>());
		reduceDriver.runTest();
	}

}	
