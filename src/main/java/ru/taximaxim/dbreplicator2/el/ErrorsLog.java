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
package ru.taximaxim.dbreplicator2.el;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;

/**
 * Класс реализации механизма логирования ошибок
 * 
 * @author volodin_aa
 *
 */
public class ErrorsLog implements ErrorsLogService, AutoCloseable{

    private static final Logger LOG = Logger.getLogger(ErrorsLog.class);

    /**
     * Имя подключения
     */
    private String baseConnName = null;
    
    private ConnectionFactory connectionFactory;

    /**
     * @return the connectionFactory
     */
    protected ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    /**
     * Подключение
     */
    private Connection connection; 

    /**
     * кешированный запрос обновления
     */
    private Map<String, PreparedStatement> statementsCache;
    
    /**
     * @return the statementsCash
     */
    protected synchronized Map<String, PreparedStatement> getStatementsCache() {
        if (statementsCache == null) {
            statementsCache = new HashMap<String, PreparedStatement>();
        }

        return statementsCache;
    }

    /**
     * Получение выражения на основе текста запроса.
     * Выражения кешируются.
     * 
     * @param query
     * @return
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private PreparedStatement getStatement(String query) 
            throws ClassNotFoundException, SQLException {
        PreparedStatement statement = getStatementsCache().get(query); 
        if (statement == null) {
            statement = getConnection().prepareStatement(query);
            getStatementsCache().put(query, statement);
        }

        return statement;
    }

    /**
     * Конструктор на основе соединения к БД 
     */
    public ErrorsLog(String baseConnName, ConnectionFactory connectionFactory) {
        this.baseConnName = baseConnName;
        this.connectionFactory = connectionFactory;
    }

    /**
     * @return the connection
     * @throws SQLException 
     * @throws ClassNotFoundException 
     */
    protected Connection getConnection() throws ClassNotFoundException, SQLException {
        if(connection==null) {
            connection = getConnectionFactory().getConnection(baseConnName);
        }
        return connection;
    }

    @Override
    public void add(Integer runnerId, String tableId, Long foreignId, String error, Throwable e) {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        printWriter.println("Подробности: ");
        e.printStackTrace(printWriter);
        printWriter.flush();
        add(runnerId, tableId, foreignId, error + "\n" + writer.toString());
    }

    @Override
    public void add(Integer runnerId, String tableId, Long foreignId, String error, SQLException e) {
        Writer writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        printWriter.println("Подробности: ");
        e.printStackTrace(printWriter);

        SQLException nextEx = e.getNextException();
        while (nextEx!=null){
            printWriter.println("Подробности: ");
            nextEx.printStackTrace(printWriter);
            nextEx = nextEx.getNextException();
        }
        add(runnerId, tableId, foreignId, error + "\n" + writer.toString());
    }

    @Override
    public void add(Integer runnerId, String tableId, Long foreignId, String error) {
        try {
            PreparedStatement statement = 
                    getStatement("INSERT INTO rep2_errors_log (id_runner, id_table, id_foreign, c_date, c_error, c_status) values (?, ?, ?, ?, ?, 0)");
            statement.setObject(1, runnerId);
            statement.setObject(2, tableId);
            statement.setObject(3, foreignId);
            statement.setTimestamp(4, new Timestamp(new Date().getTime()));
            statement.setString(5, error);
            statement.execute(); 
        } catch (Throwable e) {
            LOG.fatal("Ошибка записи в rep2_errors_log:", e);
            LOG.error(error);
        }     
    }
    
    protected int addIsNull(StringBuffer query, Object value) {
        if (value == null) {
            query.append(" IS NULL");
            return 0;
        } else {
            query.append("=?");
            return 1;
        }   
    }
    
    @Override
    public void setStatus(Integer runnerId, String tableId, Long foreignId, int status) {
        StringBuffer updateQuery = new StringBuffer("UPDATE rep2_errors_log SET c_status = ? WHERE c_status<> ?  AND id_runner");
        try {
            int runnerIdPos = addIsNull(updateQuery, runnerId);
            
            updateQuery.append(" AND id_table");
            int tableIdPos = addIsNull(updateQuery, tableId);
            
            updateQuery.append(" AND id_foreign");
            int foreignIdPos = addIsNull(updateQuery, foreignId);

            PreparedStatement statement = getStatement(updateQuery.toString());
            
            statement.setInt(1, status);
            statement.setInt(2, status);
            
            if (runnerIdPos != 0) {
                statement.setInt(2 + runnerIdPos, runnerId);
            }
            
            if (tableIdPos != 0) {
                statement.setString(2 + runnerIdPos + tableIdPos, tableId);
            }
            
            if (foreignIdPos != 0) {
                statement.setLong(2 + runnerIdPos + tableIdPos + foreignIdPos, foreignId);
            }

            statement.execute();
        } catch (Throwable e) {
            LOG.fatal(
                    String.format(
                            "Ошибка при установки статуса в rep2_errors_log: runnerId=[%s], tableId=[%s], foreignId=[%s], c_status=%s", 
                            runnerId, tableId, foreignId, status), 
                    e);
        }       
    }
    
    @Override
    public void close() {
        for (PreparedStatement statement: getStatementsCache().values()) {
            close(statement);
        }
        getStatementsCache().clear();

        close(connection);
    }
    
    /**
     * Закрыть PreparedStatement
     * @param statement
     * @throws SQLException
     */
    private void close(PreparedStatement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.warn("Ошибка при попытке закрыть 'statement.close()': ", e);
            }
        }
    }
    
    /**
     * Закрыть PreparedStatement
     * @param statement
     * @throws SQLException
     */
    private void close(Connection conn) {
        if (conn!=null) {
            try {
                conn.close();
            } catch (SQLException e) {
                LOG.warn("Ошибка при попытке закрыть 'connection.close()': ", e);
            }
        }
    }
}