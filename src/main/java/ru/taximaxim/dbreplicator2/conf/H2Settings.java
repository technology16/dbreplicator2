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

package ru.taximaxim.dbreplicator2.conf;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.h2.tools.Server;

public class H2Settings extends AbstractSettings {

	protected Connection connection;
	
	protected Server server;
	
	/**
	 * Инициализирует новый объект для работы с настройками приложения.
	 * 
	 * @param jdbc_url JDBC URL
	 * @param jdbc_user Пользователь БД
	 * @param jdbc_password Пароль БД
	 * @param tcp_port Порт сервер (параметр используется только если установлен startServer в true)
	 * @param startServer Запускать ли TCP сервер БД
	 * 
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public H2Settings(String jdbc_url, String jdbc_user, String jdbc_password, 
			String tcp_port, boolean startServer) throws ClassNotFoundException, SQLException {
		
		Class.forName("org.h2.Driver");

		if (startServer) {
			server = Server.createTcpServer(
	    		new String[] { "-tcpPort", tcp_port, "-tcpAllowOthers" }).start();
		}
		
		connection = DriverManager.getConnection(jdbc_url, jdbc_user, jdbc_password);
		connection.setAutoCommit(false);
	}
	
	public void save() throws ConfigurableError {
		try {
			
			if (isDirty() == false)
				return;
			
			if (isDbInitialized() == false)
				initializeDb();

			synchronized (connection) {
				try (PreparedStatement statement = connection.prepareStatement("merge into settings key(_key) values (?, ?)")) {
					for (Map.Entry<String, String> setting : settings.entrySet())
					{
						statement.setString(1, setting.getKey());
						statement.setString(2, setting.getValue());
						statement.addBatch();
					}
					statement.execute();
					connection.commit();
				}
			}
			
		} catch (Exception e) {
			throw new ConfigurableError("Ошибка во время сохранения настроек", e);
		}
	}

	public void load() throws ConfigurableError {
		try {
			if (isDbInitialized() == false)
				initializeDb();

			synchronized (connection) {
				try (PreparedStatement statement = connection.prepareStatement("select * from settings")) {
					
					ResultSet resultSet = statement.executeQuery();
					
					settings.clear();
					
					while (resultSet.next()) {
						settings.put(resultSet.getString(1), resultSet.getString(2));
					}
				}
			}
			
		} catch (Exception e) {
			throw new ConfigurableError("Ошибка во время загрузки настроек", e);
		}
	}
	
	/**
	 * Проверяет инициализирована ли БД для сохранения настроек.
	 * 
	 * @return true в случае, если БД уже была инициализирована
	 * @throws SQLException
	 */
	protected boolean isDbInitialized() throws SQLException {
		
		boolean retval = false;
		
		synchronized (connection) {
			try (Statement statement = connection.createStatement()) {
				ResultSet resultSet = 
						statement.executeQuery("select 1 from information_schema.tables where table_name = 'SETTINGS' and table_schema = 'PUBLIC'");
				
				if (resultSet.next())
					retval = (resultSet.getInt(1) == 1);
			}
		}
		return retval;
	}
	
	/**
	 * Инициализирует базу данных для использования её в качестве настроек
	 * 
	 * @throws SQLException
	 */
	protected void initializeDb() throws SQLException {
		
		synchronized (connection) {
			try (Statement statement = connection.createStatement()) {
				statement.addBatch("create table settings (_key varchar_ignorecase(256) primary key, _value text)");
				statement.executeBatch();
				
				connection.commit();
			}
		}
	}  
}

