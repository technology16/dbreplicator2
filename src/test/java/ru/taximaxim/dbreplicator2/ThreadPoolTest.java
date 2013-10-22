package ru.taximaxim.dbreplicator2;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ThreadPoolTest {

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testPool() {
		ThreadPool threadPool = new ThreadPool(3);
		threadPool.start("1");
		threadPool.start("2");
		threadPool.start("3");
		threadPool.start("4");
		threadPool.start("5");
		threadPool.start("6");
		threadPool.start("7");
		threadPool.start("8");
		threadPool.start("9");
		threadPool.start("a");
		threadPool.start("b");
		threadPool.start("c");
		threadPool.start("d");
		threadPool.shutdown();
	}

}
