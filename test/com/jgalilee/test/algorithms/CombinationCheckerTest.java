package com.jgalilee.test.algorithms;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.jgalilee.algorithms.CombinationChecker;

public class CombinationCheckerTest {
	
	private static String TEST_FILENAME = "test.txt";
	private static String TEST_DELIMITER = "	";
	
	private CombinationChecker checker = null;
	private List<URI> uris = null;
	
	@Before
	@SuppressWarnings("resource")
	public void setUp() throws URISyntaxException, IOException {
		String[] transactionItems = new String[] { "A", "B", "C", "D" };
		uris = Arrays.asList(new URI(TEST_FILENAME));
		PrintWriter out = new PrintWriter(TEST_FILENAME);
		for (String s : transactionItems) {
			out.println(s);
		}
		out.flush();
		checker = new CombinationChecker(TEST_DELIMITER);
		checker.load(uris);
		return;
	}
	
	@After
	public void cleanUp() {
		File file = new File(TEST_FILENAME);
		file.delete();
	}
	
	@Test
	public void testValidItemset() {	
		List<String> validItemset = Arrays.asList(new String[] { "A" });
		assertEquals(checker.valid(validItemset), true);
	}
	
	@Test
	public void testInvalidItemset() {	
		List<String> invalidItemset = Arrays.asList(new String[] { "A", "B" });
		assertEquals(checker.valid(invalidItemset), false);
	}

}
