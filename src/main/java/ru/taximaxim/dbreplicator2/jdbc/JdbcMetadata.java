/**
 * Функции для работы с метаданными
 * 
 */
package ru.taximaxim.dbreplicator2.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

//import com.microsoft.sqlserver.jdbc.SQLServerConnection;

/**
 * @author volodin_aa
 *
 */
public final class JdbcMetadata {
	/**
	 * Сиглетон 
	 */
	private JdbcMetadata() {
		
	}
	
	/**
	 * Функция получения списка колонок таблицы на основе метаданных БД
	 * 
	 * @param connection соединение к целевой БД
	 * @param tableName имя таблицы
	 * @return список колонок таблицы
	 * @throws SQLException
	 */
	public static List<String> getColumnsList(Connection connection, String tableName) throws SQLException{
		// Получаем список колонок
		List<String> colsList = new ArrayList<String>();
		DatabaseMetaData metaData = connection.getMetaData();
		ResultSet colsResultSet = metaData.getColumns(null, null, tableName, null);
		try {
			while (colsResultSet.next()) {
				colsList.add(colsResultSet.getString("COLUMN_NAME"));
			}
		} finally {
			colsResultSet.close();
		}
		
		return colsList;
	}
	
	/**
	 * Функция получения списка ключевых колонок таблицы на основе метаданных БД
	 * 
	 * @param connection соединение к целевой БД
	 * @param tableName имя таблицы
	 * @return список ключевых колонок таблицы
	 * @throws SQLException
	 */
	public static List<String> getPrimaryColumnsList(Connection connection, String tableName) throws SQLException{
		// Получаем список ключевых колонок
		List<String> primaryKeyColsList = new ArrayList<String>();
		DatabaseMetaData metaData = connection.getMetaData();
		ResultSet primaryKeysResultSet = null;
/*		if (connection instanceof SQLServerConnection) {
			primaryKeysResultSet = metaData.getPrimaryKeys(null, "DBO", tableName);
		} else {*/
			primaryKeysResultSet = metaData.getPrimaryKeys(null, null, tableName);
/*		}*/
		try {
			while (primaryKeysResultSet.next()) {
				primaryKeyColsList.add(primaryKeysResultSet.getString("COLUMN_NAME"));
			}
		} finally {
			primaryKeysResultSet.close();
		}

		return primaryKeyColsList;
	}

}
