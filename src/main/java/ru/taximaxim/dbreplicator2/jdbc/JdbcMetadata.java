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
     * @param connection
     *            соединение к целевой БД
     * @param tableName
     *            имя таблицы
     * @return список колонок таблицы
     * @throws SQLException
     */
    public static List<String> getColumnsList(Connection connection, String tableName)
            throws SQLException {
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
     * @param connection
     *            соединение к целевой БД
     * @param tableName
     *            имя таблицы
     * @return список ключевых колонок таблицы
     * @throws SQLException
     */
    public static List<String> getPrimaryColumnsList(Connection connection,
            String tableName) throws SQLException {
        // Получаем список ключевых колонок
        List<String> primaryKeyColsList = new ArrayList<String>();
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet primaryKeysResultSet = null;
        /*
         * if (connection instanceof SQLServerConnection) { primaryKeysResultSet
         * = metaData.getPrimaryKeys(null, "DBO", tableName); } else {
         */
        primaryKeysResultSet = metaData.getPrimaryKeys(null, null, tableName);
        /* } */
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
