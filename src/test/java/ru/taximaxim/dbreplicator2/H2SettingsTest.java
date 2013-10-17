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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.conf.ConfigurableError;
import ru.taximaxim.dbreplicator2.conf.H2Settings;
import ru.taximaxim.dbreplicator2.conf.Settings;

public class H2SettingsTest {

	private static final String TCP_PORT = "8084";
	private static final String JDBC_URL      = "jdbc:h2:tcp://localhost:" + TCP_PORT + "/~/H2Settings";
	private static final String JDBC_USER     = "sa";
	private static final String JDBC_PASSWORD = "";
	
	private static Settings settings;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		settings = new H2Settings(JDBC_URL, 
				JDBC_USER, JDBC_PASSWORD, TCP_PORT, true);
	}

	@Test
	public void testSettings() throws ConfigurableError {
		String val1 = "_my_value";
		
		settings.setValue("_my_key", val1);

		// Проверяем сохранение в БД
		settings.save();
		String val2 = settings.getValue("_my_key");
		Assert.assertEquals(val1, val2);

		// Проверяем восстановление
		settings.load();
		val2 = settings.getValue("_my_key");
		Assert.assertEquals(val1, val2);		

	}
}
