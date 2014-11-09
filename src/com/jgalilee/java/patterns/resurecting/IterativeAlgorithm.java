package com.jgalilee.java.patterns.resurecting;

public interface IterativeAlgorithm {

	/**
	 * Returns the next iteration. Assumes that the last iteration completed
	 * successfully.
	 * @return The next iteration.
	 */
	public Iteration step();

	/**
	 * Determines if the algorithm has converged.
	 * @return True if the algorithm has converged, false otherwise.
	 */
	public Boolean converged();

	/**
	 * Persists the state of the algorithm to stable storage so that it can be
	 * recovered at a later stage by the resurrect method.
	 * @return True if the commit method succeeded, false otherwise.
	 */
	public Boolean commit();

	/**
	 * Restores the state of the algorithm that was persisted using the commit
	 * method.
	 * @return True if the resurrect method succeeded, false otherwise.
	 */
	public Boolean resurrect();

	/**
	 * Return the interger representation of the step (iteration) that the
	 * algorithm is up to.
	 * @return The current step number.
	 */
	public Integer getIteration();

}
