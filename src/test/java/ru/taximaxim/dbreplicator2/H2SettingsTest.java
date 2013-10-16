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
