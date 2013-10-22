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
		
		threadPool.restart();
		threadPool.start("11");
		threadPool.start("22");
		threadPool.start("33");
		threadPool.start("44");
		threadPool.start("55");
		
		threadPool.restart();
		threadPool.start("111");
		threadPool.start("222");
		threadPool.start("333");
		threadPool.start("444");
		threadPool.start("555");
		
		threadPool.restart();
		threadPool.start("1111");
		threadPool.start("2222");
		threadPool.start("3333");
		threadPool.start("4444");
		threadPool.start("5555");
	}

}
