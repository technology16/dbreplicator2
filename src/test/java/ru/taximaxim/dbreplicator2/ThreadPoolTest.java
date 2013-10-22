/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2013 Technologiya
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package ru.taximaxim.dbreplicator2;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class ThreadPoolTest {
	
	private static ThreadPool threadPool = null;
	private static int count = 5;
	
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
		
		//threadPool.restart();
		threadPool.start("11");
		threadPool.start("22");
		threadPool.start("33");
		threadPool.start("44");
		threadPool.start("55");
		
		//threadPool.restart();
		threadPool.start("111");
		threadPool.start("222");
		threadPool.start("333");
		threadPool.start("444");
		threadPool.start("555");
		
		//threadPool.restart();
		threadPool.start("1111");
		threadPool.start("2222");
		threadPool.start("3333");
		threadPool.start("4444");
		threadPool.start("5555");
	}

}
