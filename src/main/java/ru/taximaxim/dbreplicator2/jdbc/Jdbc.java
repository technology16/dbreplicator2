/**
 * Утилиты работы с JDBC данными
 */
package ru.taximaxim.dbreplicator2.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * @author volodin_aa
 *
 */
public final class Jdbc {
	private final static Logger LOG = Logger.getLogger(Jdbc.class.getName());
	
	/**
	 * Сиглетон 
	 */
	private Jdbc() {
		
	}
	
	/**
	 * Функция заполнения параметров подготовленного запроса на основе строки данных из ResultSet
	 * 
	 * @param statement запрос приемник
	 * @param data источник данных
	 * @param columnsList список колонок для заполнения
	 * @throws SQLException
	 */
	public static void fillStatementFromResultSet(PreparedStatement statement, ResultSet data, 
			List<String> columnsList) throws SQLException{
		LOG.debug(statement);
		
		int columnIndex = 1;
		for (String columnName: columnsList){
			LOG.trace(data.getObject(columnName));
			statement.setObject(columnIndex, data.getObject(columnName));
			columnIndex++;
		}
	}
	
	/**
	 * Функция заполнения параметров подготовленного запроса на основе строки данных из Map<String, Object>
	 * 
	 * @param statement запрос приемник
	 * @param data источник данных
	 * @param columnsList список колонок для заполнения
	 * @throws SQLException
	 */
	public static void fillStatementFromResultSet(PreparedStatement statement, Map<String, Object> data, 
			List<String> columnsList) throws SQLException{
		LOG.debug(statement);
		
		int columnIndex = 1;
		for (String columnName: columnsList){
			LOG.trace(data.get(columnName));
			statement.setObject(columnIndex, data.get(columnName));
			columnIndex++;
		}
	}
	
	/**
	 * Функция заполнения Map на основе данных строки ResultSet
	 * 
	 * @param data исходные данные
	 * @param columnsList целевой список колонок
	 * @return Map: имя колонки - значение
	 * @throws SQLException
	 */
	public static Map<String, Object> resultSetToMap(ResultSet data, List<String> columnsList) throws SQLException{
		Map<String, Object> result = new HashMap<String, Object>();
		
		for (String columnName: columnsList){
			result.put(columnName, data.getObject(columnName));
		}
		
		return result;
	}

}
