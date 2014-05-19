package com.jgalilee.test.algorithms;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.jgalilee.algorithms.CombinationGenerator;

public class CombinationGeneratorTest {

	private CombinationGenerator<String> comb = new CombinationGenerator<String>();
	
	@Test
	public void testCombinations() {
		List<String> items = Arrays.asList("a", "b", "c");
		comb.reset(2, items);
		int combinations = 0;
		while (comb.hasNext()) {
			combinations++;
			List<String> current = comb.next();
			switch (combinations) {
			case 1:
				assertArrayEquals(new String[] { "a", "b" }, current.toArray());
				break;
			case 2:
				assertArrayEquals(new String[] { "a", "c" }, current.toArray());
				break;
			case 3:
				assertArrayEquals(new String[] { "b", "c" }, current.toArray());
				break;
			}
		}
		assertEquals(3, combinations);
	}

}
