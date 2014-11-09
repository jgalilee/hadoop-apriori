package com.jgalilee.java.patterns.resurecting;

public interface Iteration {

	/**
	 * Run the iteration job. Report the success or failure of the job.
	 * @return True if the iteration was a success, false otherwise.
	 */
	public Boolean run();

	/**
	 * Retry the iteration job. Report the success or failure of the retry attempt.
	 * @return True if the iteration was a success, false otherwise.
	 */
	public Boolean retry();

}
