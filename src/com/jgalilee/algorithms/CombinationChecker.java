package com.jgalilee.algorithms;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 
 * @author jgalilee
 *
 */
public class CombinationChecker {

	protected String delimiter = " ";
	protected Set<List<String>> candidatePreviousK = null;
	
	/**
	 * 
	 * @param delimiter
	 */
	public CombinationChecker(String delimiter) {
		this.delimiter = delimiter;
		candidatePreviousK = new HashSet<List<String>>();
	}
	
	/**
	 * 
	 * @param uris
	 * @throws IOException
	 */
	public void load(List<URI> uris) throws IOException {
		candidatePreviousK.clear();
		candidatePreviousK = new HashSet<List<String>>();
		System.out.printf("Loading %s cache file(s).\n", uris);
		for (URI uri : uris) {
			System.out.printf("Reading cache file %s\n", uri.toString());
			File file = new File(uri.toString());
			FileReader fr = new FileReader(file);
			BufferedReader br = new BufferedReader(fr);
			String line = null;
			while (null != (line = br.readLine())) {
				System.out.printf("Adding %s\n", line);
				add(line);
			}
			br.close();
		}
	}
	
	/**
	 * Split the transaction string on the delimiter and add it to the previous
	 * k candidate item sets.
	 * @param transcation
	 */
	public void add(String txn) {
		this.candidatePreviousK.add(Arrays.asList(txn.split(delimiter)));
	}

	/**
	 * Checks if the itemset is valid based on whether or not it is contained
	 * within the previous k itemsets.
	 * @param itemset The itemset to check the validity of.
	 * @return True if the itemset is valid, false otherwise.
	 */
	public boolean valid(List<String> itemset) {
		return candidatePreviousK.contains(itemset);
	}
	
}
