package com.jgalilee.test.hadoop.apriori;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.jgalilee.hadoop.apriori.AprioriCombiner;

public class AprioriCombinerTest {

	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

	@Before
	public void setUp() {
		AprioriCombiner reducer = new AprioriCombiner();
		reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
		reduceDriver.setReducer(reducer);
	}
	
	@Test
	public void testReduceSupported() throws IOException {
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		values.add(new IntWritable(1));
		reduceDriver.withInput(new Text("A"), values);
		reduceDriver.withOutput(new Text("A"), new IntWritable(2));
		reduceDriver.runTest();
	}

}	
