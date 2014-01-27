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
package ru.taximaxim.dbreplicator2.replica.strategies.replication.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Заготовка класса для работы с реплицируемыми данными
 * 
 * @author volodin_aa
 *
 */
public class DataServiceSkeleton implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(DataServiceSkeleton.class);
    private Connection connection;

    /**
     * Конструктор на основе подключения к БД
     * 
     * @param connection подключение к БД
     */
    public DataServiceSkeleton(Connection connection) {
        this.connection = connection;
    }

    /**
     * Функция получение подключения к БД
     * 
     * @return the connection
     */
    protected Connection getConnection() {
        return connection;
    }

    @Override
    public void close() throws SQLException {
        
    }
    
    /**
     * Закрыть  Map<?, PreparedStatement>
     * 
     * @param sqlStatements
     * @throws SQLException
     */
    public void close(Map<?, PreparedStatement> sqlStatements) {
        for (PreparedStatement statement : sqlStatements.values()) {
            if (statement != null) {
                close(statement);
            }
        }
        sqlStatements.clear();
    }
    
    /**
     * Закрыть PreparedStatement
     * 
     * @param statement
     * @throws SQLException
     */
    public void close(PreparedStatement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.warn("Ошибка при попытке закрыть 'statement.close()': ", e);
            }
        }
    } 
}