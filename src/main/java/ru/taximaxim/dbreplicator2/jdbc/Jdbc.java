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
 * Утилиты работы с JDBC данными
 */
package ru.taximaxim.dbreplicator2.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * @author volodin_aa
 * 
 */
public final class Jdbc {
    private static final Logger LOG = Logger.getLogger(Jdbc.class.getName());

    /**
     * Сиглетон
     */
    private Jdbc() {

    }

    /**
     * Функция заполнения параметров подготовленного запроса на основе строки
     * данных из ResultSet
     * 
     * @param statement
     *            запрос приемник
     * @param data
     *            источник данных
     * @param columnsList
     *            список колонок для заполнения
     * @throws SQLException
     */
    public static void fillStatementFromResultSet(PreparedStatement statement,
            ResultSet data, Collection<String> columnsList) throws SQLException {
        LOG.debug(statement);

        int columnIndex = 1;
        for (String columnName : columnsList) {
            LOG.trace(data.getObject(columnName));
            statement.setObject(columnIndex, data.getObject(columnName));
            columnIndex++;
        }
    }

    /**
     * Преобразование resultSet в строку
     * @param data
     *            источник данных
     * @param columnsList
     *            список колонок для заполнения
     * @return
     * @throws SQLException
     */
    public static String resultSetToString(ResultSet data, 
            Collection<String> columnsList) throws SQLException {
        StringBuffer str  = new StringBuffer();
        for (String columnName : columnsList) {
            str.append("[ col ").append(columnName).append(" = ")
                .append(data.getString(columnName)).append(" ] ");
        }
        return str.toString();
    }
    
    /**
     * Функция заполнения параметров подготовленного запроса на основе строки
     * данных из Map<String, Object>
     * 
     * @param statement
     *            запрос приемник
     * @param data
     *            источник данных
     * @param columnsList
     *            список колонок для заполнения
     * @throws SQLException
     */
    public static void fillStatementFromResultSet(PreparedStatement statement,
            Map<String, Object> data, Collection<String> columnsList) throws SQLException {
        LOG.debug(statement);

        int columnIndex = 1;
        for (String columnName : columnsList) {
            LOG.trace(data.get(columnName));
            statement.setObject(columnIndex, data.get(columnName));
            columnIndex++;
        }
    }

    /**
     * Функция заполнения Map на основе данных строки ResultSet
     * 
     * @param data
     *            исходные данные
     * @param columnsList
     *            целевой список колонок
     * @return Map: имя колонки - значение
     * @throws SQLException
     */
    public static Map<String, Object> resultSetToMap(ResultSet data,
            Collection<String> columnsList) throws SQLException {
        Map<String, Object> result = new HashMap<String, Object>();

        for (String columnName : columnsList) {
            result.put(columnName, data.getObject(columnName));
        }

        return result;
    }

}
