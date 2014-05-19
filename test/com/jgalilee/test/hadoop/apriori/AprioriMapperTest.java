package com.jgalilee.test.hadoop.apriori;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.jgalilee.hadoop.apriori.AprioriMapper;


/**
 * Test case for the Apriori mapper, assume that the test case has the support
 * threshold (minsup) equal to 3. Assume that all iterations starts from 1,
 * not 0.
 *
 * There are five transactions
 * (http://www.cse.cuhk.edu.hk/~taoyf/course/cmsc5724/notes/asso-apriori.pdf)
 * 
 * 1	Bread Milk
 * 2	Beer Diaper Eggs
 * 3	Beer Bread Coke Diaper Milk
 * 4	Beer Bread Diaper Milk
 * 5	Bread Coke Diaper Milk
 *
 * @author jgalilee
 *
 */
public class AprioriMapperTest {
	
	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private List<String> filenames = new ArrayList<String>(); 
	
	@SuppressWarnings({ "resource", "rawtypes" })
	private void prepareIteration(int k, String... previousKItems)
			throws URISyntaxException, IOException {
		Configuration conf = mapDriver.getConfiguration();
		conf.clear();
		conf.set("k", String.valueOf(k));
		if (null != previousKItems && previousKItems.length > 0) {
			String filename = "k" + k + ".txt";
			PrintWriter out = new PrintWriter(filename);
			for (String item : previousKItems) {
				out.println(item);
			}
			out.flush();
			filenames.add(filename);
			URI fakeFileURI = new URI(filename);
			mapDriver.addCacheFile(fakeFileURI);
			Context context = mapDriver.getContext();
			Mockito.when(context.getCacheFiles()).
				thenReturn(new URI[] { fakeFileURI });
		}
		return;
	}
	
	@Before
	public void setUp() throws FileNotFoundException, URISyntaxException {
		AprioriMapper mapper = new AprioriMapper();	
		mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
		mapDriver.setMapper(mapper);
		mapDriver.withInput(new LongWritable(0),
				new Text("1	Bread Milk"));
		mapDriver.withInput(new LongWritable(2),
				new Text("2	Beer Diaper Eggs"));
		mapDriver.withInput(new LongWritable(3),
				new Text("3	Beer Bread Coke Diaper Milk"));
		mapDriver.withInput(new LongWritable(3),
				new Text("4	Beer Bread Diaper Milk"));
		mapDriver.withInput(new LongWritable(3),
				new Text("5	Bread Coke Diaper Milk"));
	}
	
	@After
	public void cleanUp() {
		for (String filename : filenames) {
			File file = new File(filename);
			file.delete();
		}
	}
	
	@Test
	public void testMapperIterationK1() throws IOException, URISyntaxException {
		prepareIteration(1);
		// Transaction 1
		mapDriver.withOutput(new Text("Bread"), new IntWritable(1));
		mapDriver.withOutput(new Text("Milk"), new IntWritable(1));
		// Transaction 2
		mapDriver.withOutput(new Text("Beer"), new IntWritable(1));
		mapDriver.withOutput(new Text("Diaper"), new IntWritable(1));
		mapDriver.withOutput(new Text("Eggs"), new IntWritable(1));
		// Transaction 3
		mapDriver.withOutput(new Text("Beer"), new IntWritable(1));
		mapDriver.withOutput(new Text("Bread"), new IntWritable(1));
		mapDriver.withOutput(new Text("Coke"), new IntWritable(1));
		mapDriver.withOutput(new Text("Diaper"), new IntWritable(1));
		mapDriver.withOutput(new Text("Milk"), new IntWritable(1));
		// Transaction 4
		mapDriver.withOutput(new Text("Beer"), new IntWritable(1));
		mapDriver.withOutput(new Text("Bread"), new IntWritable(1));
		mapDriver.withOutput(new Text("Diaper"), new IntWritable(1));
		mapDriver.withOutput(new Text("Milk"), new IntWritable(1));
		// Transaction 5
		mapDriver.withOutput(new Text("Bread"), new IntWritable(1));
		mapDriver.withOutput(new Text("Coke"), new IntWritable(1));
		mapDriver.withOutput(new Text("Diaper"), new IntWritable(1));
		mapDriver.withOutput(new Text("Milk"), new IntWritable(1));
		mapDriver.runTest();
	}
	
	@Test
	public void testMapperIterationK2() throws IOException, URISyntaxException {
		prepareIteration(2, "Bread", "Milk", "Beer", "Diaper");
		// Transaction 1
		mapDriver.withOutput(new Text("Bread Milk"), new IntWritable(1));
		// Transaction 2
		mapDriver.withOutput(new Text("Beer Diaper"), new IntWritable(1));
		// Transaction 3
		mapDriver.withOutput(new Text("Beer Bread"), new IntWritable(1));
		mapDriver.withOutput(new Text("Beer Diaper"), new IntWritable(1));
		mapDriver.withOutput(new Text("Beer Milk"), new IntWritable(1));
		mapDriver.withOutput(new Text("Bread Diaper"), new IntWritable(1));
		mapDriver.withOutput(new Text("Bread Milk"), new IntWritable(1));
		mapDriver.withOutput(new Text("Diaper Milk"), new IntWritable(1));
		// Transaction 4
		mapDriver.withOutput(new Text("Beer Bread"), new IntWritable(1));
		mapDriver.withOutput(new Text("Beer Diaper"), new IntWritable(1));
		mapDriver.withOutput(new Text("Beer Milk"), new IntWritable(1));
		mapDriver.withOutput(new Text("Bread Diaper"), new IntWritable(1));
		mapDriver.withOutput(new Text("Bread Milk"), new IntWritable(1));
		mapDriver.withOutput(new Text("Diaper Milk"), new IntWritable(1));
		// Transaction 5
		mapDriver.withOutput(new Text("Bread Diaper"), new IntWritable(1));
		mapDriver.withOutput(new Text("Bread Milk"), new IntWritable(1));
		mapDriver.withOutput(new Text("Diaper Milk"), new IntWritable(1));
		mapDriver.runTest();
	}
	
	@Test
	public void testMapperIterationK3() throws IOException, URISyntaxException {
		prepareIteration(3, "Bread Milk", "Bread Diaper", "Diaper Milk",
				"Beer Diaper");
		// Transaction 1 (No output |Txn1| < k)
		// Transaction 2 (No output, nothing frequent for k-1)
		// Transaction 3
		mapDriver.withOutput(new Text("Bread Diaper Milk"), new IntWritable(1));
		// Transaction 4
		mapDriver.withOutput(new Text("Bread Diaper Milk"), new IntWritable(1));
		// Transaction 5
		mapDriver.withOutput(new Text("Bread Diaper Milk"), new IntWritable(1));
		mapDriver.runTest();
	}
	
}
