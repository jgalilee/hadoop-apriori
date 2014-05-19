package com.jgalilee.algorithms;

import java.util.List;

public class CombinatorialException extends Exception {

	private static String DEFAULT_MESSAGE = "Unable to choose %d from %s";
	
	public CombinatorialException(int k, List<?> items) {
		super(String.format(DEFAULT_MESSAGE, k, items));
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 5506360474953356760L;

}
