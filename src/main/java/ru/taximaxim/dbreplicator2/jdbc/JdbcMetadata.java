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

/**
 * Функции для работы с метаданными
 * 
 */
package ru.taximaxim.dbreplicator2.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author volodin_aa
 * 
 */
public final class JdbcMetadata {

    private static final String COLUMN_NAME = "COLUMN_NAME";
    private static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
    private static final String IS_NULLABLE = "IS_NULLABLE";
    private static final String YES = "YES";
    private static final String DATA_TYPE = "DATA_TYPE";
    
    /**
     * Сиглетон
     */
    private JdbcMetadata() {

    }

    /**
     * Функция получения списка колонок таблицы на основе метаданных БД
     * 
     * @param connection
     *            соединение к целевой БД
     * @param tableName
     *            имя таблицы
     * @return список колонок таблицы
     * @throws SQLException
     */
    public static Set<String> getColumns(Connection connection, String tableName)
            throws SQLException {
        // Получаем список колонок
        Set<String> colsList = new HashSet<String>();
        DatabaseMetaData metaData = connection.getMetaData();
        String schemaName = null;
        String tabName = tableName;
        String[] s = tableName.split("\\.");
        if(s.length==2){
            tabName = s[1];
            schemaName = s[0];
        }
        try (ResultSet colsResultSet = metaData.getColumns(null, schemaName, tabName, null);) {
            while (colsResultSet.next()) {
                colsList.add(colsResultSet.getString(COLUMN_NAME).toUpperCase());
            }
        }

        return colsList;
    }

    /**
     * Функция получения списка колонок таблицы на основе метаданных ResultSet
     * 
     * @param result
     *            набор результатов
     * @param tableName
     *            имя таблицы
     * @return список колонок таблицы
     * @throws SQLException
     */
    public static Set<String> getColumns(ResultSet result)
            throws SQLException {
        // Получаем список колонок
        Set<String> colsList = new HashSet<String>();
        ResultSetMetaData metaData = result.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
            colsList.add(metaData.getColumnName(i).toUpperCase());
        }

        return colsList;
    }

    /**
     * Функция получения списка ключевых колонок таблицы на основе метаданных БД
     * 
     * @param connection
     *            соединение к целевой БД
     * @param tableName
     *            имя таблицы
     * @return список ключевых колонок таблицы
     * @throws SQLException
     */
    public static Set<String> getPrimaryColumns(Connection connection,
            String tableName) throws SQLException {
        // Получаем список ключевых колонок
        Set<String> primaryKeyColsList = new HashSet<String>();
        DatabaseMetaData metaData = connection.getMetaData();
        String schemaName = null;
        String tabName = tableName;
        String[] s = tableName.split("\\.");
        if(s.length==2){
            tabName = s[1];
            schemaName = s[0];
        }
        try (ResultSet primaryKeysResultSet = metaData.getPrimaryKeys(null, schemaName, tabName);) {
            while (primaryKeysResultSet.next()) {
                primaryKeyColsList.add(primaryKeysResultSet.getString(COLUMN_NAME).toUpperCase());
            }
        }

        return primaryKeyColsList;
    }
    
    /**
     * Функция получения списка AUTO INCREMENT колонок таблицы на основе метаданных БД
     * 
     * @param connection
     *            соединение к целевой БД
     * @param tableName
     *            имя таблицы
     * @return список AUTO INCREMENT колонок таблицы
     * @throws SQLException
     */
    public static Set<String> getIdentityColumns(Connection connection,
            String tableName) throws SQLException {
        // Получаем список колонок
        Set<String> colsList = new HashSet<String>();
        DatabaseMetaData metaData = connection.getMetaData();
        String schemaName = null;
        String tabName = tableName;
        String[] s = tableName.split("\\.");
        if(s.length==2){
            tabName = s[1];
            schemaName = s[0];
        }
        try (ResultSet colsResultSet = metaData.getColumns(null, schemaName, tabName, null);) {
            while (colsResultSet.next()) {
                if (colsResultSet.getString(IS_AUTOINCREMENT).equalsIgnoreCase(YES)) {
                    colsList.add(colsResultSet.getString(COLUMN_NAME).toUpperCase());
                }
            }
        }

        return colsList;
    }
    
    /**
     * Функция получения набора колонок таблицы с возможностью вставки значений 
     * NULL на основе метаданных БД
     * 
     * @param connection
     *            соединение к целевой БД
     * @param tableName
     *            имя таблицы
     * @return набор колонок таблицы с возможностью вставки значений NULL
     * @throws SQLException
     */
    public static Set<String> getNullableColumns(Connection connection,
            String tableName) throws SQLException {
        // Получаем список колонок
        Set<String> cols = new HashSet<String>();
        String schemaName = null;
        String tabName = tableName;
        String[] s = tableName.split("\\.");
        if(s.length==2){
            tabName = s[1];
            schemaName = s[0];
        }
        DatabaseMetaData metaData = connection.getMetaData();
        try (ResultSet colsResultSet = metaData.getColumns(null, schemaName, tabName, null);) {
            while (colsResultSet.next()) {
                if (colsResultSet.getString(IS_NULLABLE).equalsIgnoreCase(YES)) {
                    cols.add(colsResultSet.getString(COLUMN_NAME).toUpperCase());
                }
            }
        }

        return cols;
    }
    
    /**
     * Получние имени колонок и их тип в ввиде целого числа
     * @param result
     * @return
     * @throws SQLException
     */
    public static Map<String, Integer> getColumnsTypes(Connection connection, String tableName) throws SQLException {
        Map<String, Integer> colsTypes = new HashMap<String, Integer>();
        DatabaseMetaData metaData = connection.getMetaData();
        String schemaName = null;
        String tabName = tableName;
        String[] s = tableName.split("\\.");
        if(s.length==2){
            tabName = s[1];
            schemaName = s[0];
        }        
        try (ResultSet colsResultSet = metaData.getColumns(null, schemaName, tabName, null);) {
            while (colsResultSet.next()) {
                colsTypes.put(colsResultSet.getString(COLUMN_NAME).toUpperCase(), colsResultSet.getInt(DATA_TYPE));
            }
        }
        return colsTypes;
    }
    
    /**
     * Функция получения списка ключевых колонок ии их типов таблицы на основе метаданных БД
     * 
     * @param connection
     *            соединение к целевой БД
     * @param tableName
     *            имя таблицы
     * @return список ключевых колонок таблицы
     * @throws SQLException
     */
    public static Map<String, Integer>  getPrimaryColumnsTypes(Connection connection,
            String tableName) throws SQLException {
        // Получаем список ключевых колонок
        Map<String, Integer> primaryKeyColsTypes = new HashMap<String, Integer>();
        Map<String, Integer> colsTypes = getColumnsTypes(connection, tableName);
        Set<String> primaryKeyCols = getPrimaryColumns(connection, tableName);
        
        for (String primaryKey: primaryKeyCols) {
            primaryKeyColsTypes.put(primaryKey, colsTypes.get(primaryKey));
        }

        return primaryKeyColsTypes;
    }
    
    /**
     * Проверка isEquals(ResultSet,ResultSet)
     * @param sourceResult
     * @param targetResult
     * @param colsName
     * @param sqlType
     * @return
     * @throws SQLException
     */
    public static boolean isEquals(ResultSet sourceResult, ResultSet targetResult, String colsName, Integer sqlType) throws SQLException {
      
       boolean sourceBoolean = sourceResult.getObject(colsName) == null;
       boolean targetBoolean = targetResult.getObject(colsName) == null;
       if (sourceBoolean != targetBoolean) {
           return false;
       } else if (sourceBoolean & targetBoolean) {
           return true;
       } else {
       
         switch (sqlType) {
            case Types.SMALLINT:
                return sourceResult.getShort(colsName) == targetResult.getShort(colsName);
            case Types.INTEGER:
                return sourceResult.getInt(colsName) == targetResult.getInt(colsName);
            case Types.TINYINT:
                return sourceResult.getByte(colsName) == targetResult.getByte(colsName);
            case Types.BIGINT:
                return sourceResult.getLong(colsName) == targetResult.getLong(colsName);
            case Types.REAL:
            case Types.FLOAT:
                return sourceResult.getFloat(colsName) == targetResult.getFloat(colsName);
            case Types.DOUBLE:
                return sourceResult.getDouble(colsName) == targetResult.getDouble(colsName);
            case Types.DECIMAL:
            case Types.NUMERIC:
                return sourceResult.getBigDecimal(colsName) == targetResult.getBigDecimal(colsName);
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.LONGNVARCHAR:
                return sourceResult.getString(colsName).equals(targetResult.getString(colsName));
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.ROWID:
                return sourceResult.getBytes(colsName) == targetResult.getBytes(colsName);
            case Types.BLOB:
                return sourceResult.getBlob(colsName) == targetResult.getBlob(colsName);
            case Types.CLOB:
                return sourceResult.getClob(colsName) == targetResult.getClob(colsName);
            case Types.BOOLEAN:
            case Types.BIT:
                return !(sourceResult.getBoolean(colsName) ^ targetResult.getBoolean(colsName));
            case Types.DATE:
                return sourceResult.getDate(colsName).getTime() == targetResult.getDate(colsName).getTime();
            case Types.TIME:
                return sourceResult.getTime(colsName).getTime() == targetResult.getTime(colsName).getTime();
            case Types.TIMESTAMP:
                return sourceResult.getTimestamp(colsName).getTime() == targetResult.getTimestamp(colsName).getTime();
            default:
                return sourceResult.getObject(colsName).equals(targetResult.getObject(colsName));
          }
      }
    } 
    
    /**
     * Установка параметров
     * @param statement
     * @param resultSet
     * @param sqlType
     * @param parameterIndex
     * @param colsName
     * @throws SQLException
     */
    public static void setOptionStatementPrimaryColumns(PreparedStatement statement, 
            ResultSet resultSet, Integer sqlType, Integer parameterIndex, String colsName) throws SQLException {
        switch (sqlType) {
        case Types.SMALLINT:
            statement.setShort(parameterIndex, resultSet.getShort(colsName));
            break;
        case Types.INTEGER:
            statement.setInt(parameterIndex, resultSet.getInt(colsName));
            break;
        case Types.TINYINT:
            statement.setByte(parameterIndex, resultSet.getByte(colsName));
            break;
        case Types.BIGINT:
            statement.setLong(parameterIndex, resultSet.getLong(colsName));
            break;
        case Types.REAL:
        case Types.FLOAT:
            statement.setFloat(parameterIndex, resultSet.getFloat(colsName));
            break;
        case Types.DOUBLE:
            statement.setDouble(parameterIndex, resultSet.getDouble(colsName));
            break;
        case Types.DECIMAL:
        case Types.NUMERIC:
            statement.setBigDecimal(parameterIndex, resultSet.getBigDecimal(colsName));
            break;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.LONGNVARCHAR:
            statement.setString(parameterIndex, resultSet.getString(colsName));
            break;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.ROWID:
            statement.setBytes(parameterIndex, resultSet.getBytes(colsName));
            break;
        case Types.BLOB:
            statement.setBlob(parameterIndex, resultSet.getBlob(colsName));
            break;
        case Types.CLOB:
            statement.setClob(parameterIndex, resultSet.getClob(colsName));
            break;
        case Types.BOOLEAN:
        case Types.BIT:
            statement.setBoolean(parameterIndex, resultSet.getBoolean(colsName));
            break;
        case Types.DATE:
            statement.setDate(parameterIndex, resultSet.getDate(colsName));
            break;
        case Types.TIME:
            statement.setTime(parameterIndex, resultSet.getTime(colsName));
            break;
        case Types.TIMESTAMP:
            statement.setTimestamp(parameterIndex, resultSet.getTimestamp(colsName));
            break;
        default:
            statement.setObject(parameterIndex, resultSet.getObject(colsName));
            break;
        }
    }
}
