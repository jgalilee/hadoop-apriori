package com.jgalilee.hadoop.apriori.util;
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
 * Simple utility class to check combinations that are frequent.
 *
 * @author jgalilee
 */
public class CombinationChecker {

	private final String RECORD_DELEMITER = "	";
	private final String ITEM_DELIMITER = " ";

	protected Set<String> candidatePreviousK = null;

	/**
	 * Creates an instantiates the hash set to manage the sets of combinations.
	 */
	public CombinationChecker() {
		candidatePreviousK = new HashSet<String>();
	}

	/**
	 * Split the transaction string on the delimiter and add it to the previous
	 * k candidate item sets.
	 *
	 * @param itemset String representation of the itemset to add into the list.
	 */
	public void add(String itemset) {
		this.candidatePreviousK.add(itemset.intern());
	}

	/**
	 * Checks if the itemset is valid based on whether or not it is contained
	 * within the previous k itemsets.
	 *
	 * @param itemset The itemset to check the validity of.
	 * @return True if the itemset is valid, false otherwise.
	 */
	public boolean valid(int[] candidate) {
		String choice = Arrays.toString(candidate);
		return candidatePreviousK.contains(choice);
	}

	/**
	 * Convert the item into an integer array of items.
	 *
	 * @return Set of items in the given string.
	 */
	public int[] getItemsFromRecord(String tuple) {
		String[] columns = tuple.split(RECORD_DELEMITER);
		if (columns.length > 1) {
			return getIntArrayFromString(columns[1]);
		}
		return CombinationGenerator.BLANK_COMBINATION;
	}

	/**
	 * Convert the string into an integer set array of items.
	 *
	 * @return Set of items in the given string.
	 */
	private int[] getIntArrayFromString(String arrayString) {
		String[] items = arrayString.split(ITEM_DELIMITER);
		if (items.length > 0) {
			Arrays.sort(items);
			int finalLength = 0;
			int[] result = new int[items.length];
			for (int i = 0, j = items.length; i < j; i++) {
				int item = Integer.parseInt(items[i]);
				if (i == 0 || (result[finalLength-1] != item)) {
					result[finalLength++] = item;
				}
			}
			result = Arrays.copyOf(result, finalLength);
			Arrays.sort(result);
			return result;
		} else {
			return CombinationGenerator.BLANK_COMBINATION;
		}
	}

	/**
	 * Converts the set array of integers back into a string.
	 *
	 * @return String representation of the itemset.
	 */
	public String getStringFromIntArray(int[] items) {
		StringBuilder sb = new StringBuilder();
		for (int i = 0, j = items.length; i < j; i ++) {
			sb.append(String.valueOf(items[i]));
			if (i < j - 1) {
				sb.append(ITEM_DELIMITER);
			}
		}
		return sb.toString();
	}

}
