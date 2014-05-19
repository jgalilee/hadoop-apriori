package com.jgalilee.algorithms;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * 
 * @author jgalilee
 *
 */
public class CombinationGenerator<T> {

	private Integer k;
	private Integer n;
	private Integer[] result;
	private Stack<Integer> stack;

	private List<T> source;
	private Integer[] next;

	/**
	 * 
	 */
	public CombinationGenerator() {
		this.stack = new Stack<Integer>();
	}


	/**
	 * 
	 * @return
	 */
	private boolean hasNextCombination() {
		return stack.size() > 0;
	}

	/**
	 * 
	 * @return
	 */
	private Integer[] nextCombination() {
		while (k != 0 && hasNextCombination()) {
			Integer index = stack.size() - 1;
			Integer value = stack.pop();
			while (value < n) {
				result[index++] = value++;
				stack.push(value);
				if (index == k) {
					return result.clone();
				}
			}
		}
		return null;
	}

	/**
	 * 
	 */
	public void reset(Integer k, List<T> source) {
		this.k = k;
		n = source.size();
		this.source = source;
		result = new Integer[k];
		stack.removeAllElements();
		stack.push(0);
		next = nextCombination();
		return;
	}

	/**
	 * 
	 * @return
	 */
	public boolean hasNext() {
		if (k == 0 || null == next || !hasNextCombination()) {
			return false;
		}
		return true;
	}

	/**
	 * 
	 * @return
	 */
	public List<T> next() {
		List<T> result = new ArrayList<T>();
		for (Integer i : next) {
			result.add(source.get(i));
		}
		next = nextCombination();
		return result;
	}
	
}