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
 *  Механизм ведения статистики по количеству переданных и ошибочных записей
 */
package ru.taximaxim.dbreplicator2.stats;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

/**
 * Класс для ведения статистики
 * 
 * @author mardanov_rm
 */
public class StatsService {
    
    /**
     * Успешно реплицированные записи 
     */
    public static final int TYPE_SUCCESS = 1; 
    
    /**
     * Ошибочные записи
     */
    public static final int TYPE_ERROR = 0; 

    private DataSource dataSource;

    /**
     * Конструктор - Сервис статистики
     * 
     * @param baseConnName
     *            - имя подключения
     */
    public StatsService(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    /**
     * Метод для записи статистики в таблицу
     * 
     * @param date
     *            - Дата записи
     * @param type
     * @param id_strategy
     * @param id_table
     * @param count
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public void writeStat(Timestamp date, int type, int strategyId, String tableName,
            int count) throws SQLException {
        try (Connection conn = dataSource.getConnection();
                PreparedStatement insertStatement = conn.prepareStatement(
                "insert into rep2_statistics (c_date, c_type, id_strategy, id_table, c_count)"
                + " values (?, ?, ?, ?, ?)");) {
            insertStatement.setTimestamp(1, date);
            insertStatement.setInt(2, type);
            insertStatement.setInt(3, strategyId);
            insertStatement.setString(4, tableName);
            insertStatement.setInt(5, count);
            insertStatement.executeUpdate();
        }
    }

    /**
     * Перевод tablesResultSetList в List<Map<String, Object>> для таблицы
     * rep2_statistics
     * 
     * @param result
     * @return
     * @throws SQLException
     */
    public List<Map<String, Object>> convertResultSetToList(ResultSet result)
            throws SQLException {
        List<Map<String, Object>> tablesResultSetList = new ArrayList<Map<String, Object>>();
        while (result.next()) {
            Map<String, Object> tablesResultSet = new HashMap<String, Object>();
            tablesResultSet.put("id_statistics", result.getInt("id_statistics"));
            tablesResultSet.put("c_date", result.getTimestamp("c_date"));
            tablesResultSet.put("c_type", result.getInt("c_type"));
            tablesResultSet.put("id_strategy", result.getInt("id_strategy"));
            tablesResultSet.put("id_table", result.getString("id_table"));
            tablesResultSet.put("c_count", result.getInt("c_count"));

            tablesResultSetList.add(tablesResultSet);
        }
        return tablesResultSetList;
    }

    /**
     * Метод для получения статистики
     * 
     * @param type
     *            - Если 0 Значит ошибка, если 1 Значит успешно реплицировалась.
     * @param tableName
     *            - Имя таблицы
     * @param dateStart
     *            - Промежуток времени с будет выведен результат
     * @param dateEnd
     *            - Промежуток времени по будет выведен результат
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public List<Map<String, Object>> getStat(int type, String tableName,
            Timestamp dateStart, Timestamp dateEnd) throws SQLException,
            ClassNotFoundException {
        try (Connection conn = dataSource.getConnection();
            PreparedStatement selectStatement = conn.prepareStatement(
                "SELECT * FROM rep2_statistics WHERE c_type = ? and id_table = ? and c_date >= ? and c_date <= ?",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);) {
            selectStatement.setInt(1, type);
            selectStatement.setString(2, tableName);
            selectStatement.setTimestamp(3, dateStart);
            selectStatement.setTimestamp(4, dateEnd);
            try (ResultSet result = selectStatement.executeQuery();) {
                return convertResultSetToList(result);
            }
        }
    }

    /**
     * Метод для получения статистики
     * 
     * @param type
     *            - Если 0 Значит ошибка, если 1 Значит успешно реплицировалась.
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public List<Map<String, Object>> getStat(int type) throws SQLException,
            ClassNotFoundException {
        try (Connection conn = dataSource.getConnection();
            PreparedStatement selectStatement = conn.prepareStatement(
                "SELECT * FROM rep2_statistics WHERE c_type = ?",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);) {
            selectStatement.setInt(1, type);
            try (ResultSet result = selectStatement.executeQuery();) {
                return convertResultSetToList(result);
            }
        }
    }

    /**
     * Метод для получения статистики
     * 
     * @param type
     *            - Если 0 Значит ошибка, если 1 Значит успешно реплицировалась.
     * @param tableName
     *            - Имя таблицы
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public List<Map<String, Object>> getStat(int type, String tableName)
            throws SQLException, ClassNotFoundException {
        try (Connection conn = dataSource.getConnection();
            PreparedStatement selectStatement = conn.prepareStatement(
               "SELECT * FROM rep2_statistics WHERE c_type = ? and id_table = ?",
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);) {
            selectStatement.setInt(1, type);
            selectStatement.setString(2, tableName);

            try (ResultSet result = selectStatement.executeQuery();) {
                return convertResultSetToList(result);
            }
        }
    }

    /**
     * Метод для получения статистики
     * 
     * @param type
     *            - Если 0 Значит ошибка, если 1 Значит успешно реплицировалась.
     * @param dateStart
     *            - Промежуток времени с будет выведен результат
     * @param dateEnd
     *            - Промежуток времени по будет выведен результат
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public List<Map<String, Object>> getStat(int type, Timestamp dateStart,
            Timestamp dateEnd) throws SQLException, ClassNotFoundException {
        try (Connection conn = dataSource.getConnection();
            PreparedStatement selectStatement = conn.prepareStatement(
                 "SELECT * FROM rep2_statistics WHERE c_type = ? and c_date >= ? and c_date <= ?",
                 ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);) {
            selectStatement.setInt(1, type);
            selectStatement.setTimestamp(2, dateStart);
            selectStatement.setTimestamp(3, dateEnd);
            try (ResultSet result = selectStatement.executeQuery();) {
                return convertResultSetToList(result);
            }
        }
    }
}
