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

package ru.taximaxim.dbreplicator2.replica.strategies.replication.workpool;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.log4j.Logger;


/**
 * @author volodin_aa
 *
 */
public class GenericWorkPoolService implements WorkPoolService, AutoCloseable{
    private static final Logger LOG = Logger.getLogger(GenericWorkPoolService.class);

    private Connection connection;
    
    /**
     * Конструктор на основе соединения к БД 
     */
    public GenericWorkPoolService(Connection connection) {
        this.connection = connection;
    }
    
    /**
     * @return the connection
     */
    protected Connection getConnection() {
        return connection;
    }

    private PreparedStatement clearWorkPoolDataStatement;

    private PreparedStatement lastOperationsStatement;
    
    private PreparedStatement incErrorsCount;

    @Override
    public PreparedStatement getLastOperationsStatement() throws SQLException {
        if (lastOperationsStatement == null) {
            lastOperationsStatement = 
                getConnection().prepareStatement("SELECT * FROM rep2_workpool_data WHERE id_superlog IN (SELECT MAX(id_superlog) AS id_superlog FROM rep2_workpool_data WHERE id_runner=? GROUP BY id_foreign, id_table ORDER BY id_superlog LIMIT ? OFFSET ?) ORDER BY id_superlog ",
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY);
        }
        
        return lastOperationsStatement;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.WorkPoolService#getLastOperations(int, int, int)
     */
    @Override
    public ResultSet getLastOperations(int runnerId, int fetchSize, int offset)
            throws SQLException {
        PreparedStatement statement = getLastOperationsStatement();
        
        statement.setInt(1, runnerId);
        // Извлекаем частями равными fetchSize 
        statement.setFetchSize(fetchSize);
        
        // По задаче #2327
        // Задаем первоначальное смещение выборки равное 0.
        // При появлении ошибочных записей будем его увеличивать на 1.
        statement.setInt(2, fetchSize);
        statement.setInt(3, offset);
        
        return statement.executeQuery();
    }

    @Override
    public PreparedStatement getClearWorkPoolDataStatement() throws SQLException {
        if (clearWorkPoolDataStatement == null) {
            clearWorkPoolDataStatement = 
                    getConnection().prepareStatement("DELETE FROM rep2_workpool_data WHERE id_runner=? AND id_foreign=? AND id_table=? AND id_superlog<=?");
        }
        
        return clearWorkPoolDataStatement;
    }
    
    /**
     * Функция удаления данных об операциях успешно реплицированной записи
     * 
     * @param deleteWorkPoolData
     * @param operationsResult
     * @throws SQLException
     */
    public void clearWorkPoolData(ResultSet operationsResult) throws SQLException {
        // Очищаем данные о текущей записи из набора данных реплики
        PreparedStatement deleteWorkPoolData = 
                getClearWorkPoolDataStatement();
        deleteWorkPoolData.setInt(1, getRunner(operationsResult));
        deleteWorkPoolData.setLong(2, getForeign(operationsResult));
        deleteWorkPoolData.setString(3, getTable(operationsResult));
        deleteWorkPoolData.setLong(4, getSuperlog(operationsResult));
        deleteWorkPoolData.addBatch();
    }

    /**
     * Функция записи информации об ошибке
     * @throws SQLException 
     */
    public void trackError(String message, SQLException e, ResultSet operation) 
            throws SQLException{
        // Формируем сообщение об ошибке
        Writer writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        e.printStackTrace(printWriter);
        
        SQLException nextEx = e.getNextException();
        while (nextEx!=null){
            printWriter.println("Подробности: ");
            nextEx.printStackTrace(printWriter);
            nextEx = nextEx.getNextException();
        }

        // Увеличиваем счетчик ошибок на 1
        if(incErrorsCount==null) {
            incErrorsCount = getConnection().prepareStatement(
               "UPDATE rep2_workpool_data SET c_errors_count = c_errors_count + 1, c_last_error=?, c_last_error_date=? WHERE id_runner=? AND id_table=? AND id_foreign=?");
        }
        incErrorsCount.setString(1, message + "\n" + writer.toString());
        incErrorsCount.setTimestamp(2, new Timestamp(new Date().getTime()));
        incErrorsCount.setInt(3, getRunner(operation));
        incErrorsCount.setString(4, getTable(operation));
        incErrorsCount.setLong(5, getForeign(operation));
        incErrorsCount.executeUpdate();
    }
    
    @Override
    public String getTable(ResultSet resultSet) throws SQLException {
        return resultSet.getString(ID_TABLE);
    }
    
    @Override
    public String getOperation(ResultSet resultSet) throws SQLException {
        return resultSet.getString(C_OPERATION);
    }
    
    @Override
    public Long getForeign(ResultSet resultSet) throws SQLException {
        return resultSet.getLong(ID_FOREIGN);
    }
    
    @Override
    public int getRunner(ResultSet resultSet) throws SQLException {
        return resultSet.getInt(ID_RUNNER);
    }
    
    @Override
    public Long getSuperlog(ResultSet resultSet) throws SQLException {
        return resultSet.getLong(ID_SUPERLOG);
    }

    @Override
    public String getPool(ResultSet resultSet) throws SQLException {
        return resultSet.getString(ID_POOL);
    }
    
    @Override
    public Timestamp getDate(ResultSet resultSet) throws SQLException {
        return resultSet.getTimestamp(C_DATE);
    }
    
    @Override
    public String getTransaction(ResultSet resultSet) throws SQLException {
        return resultSet.getString(ID_TRANSACTION);
    }

    @Override
    public void close() {
        close(clearWorkPoolDataStatement);
        close(lastOperationsStatement);
        close(incErrorsCount);
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
}
