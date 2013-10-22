package ru.taximaxim.dbreplicator2;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ThreadPoolTest {
	
	private static ThreadPool threadPool = null;
	private static int count = 3;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		threadPool = new ThreadPool(count);
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		threadPool.shutdown();
	}

	@Test
	public void testPool() {
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
	}

}
