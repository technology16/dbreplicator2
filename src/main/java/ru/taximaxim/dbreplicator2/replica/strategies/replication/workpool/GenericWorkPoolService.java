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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.el.ErrorsLogService;


/**
 * @author volodin_aa
 *
 */
public class GenericWorkPoolService implements WorkPoolService, AutoCloseable {
    private static final Logger LOG = Logger.getLogger(GenericWorkPoolService.class);

    private Connection connection;
    private ErrorsLogService errorsLog;
    
    /**
     * @return the errorsLog
     */
    protected ErrorsLogService getErrorsLog() {
        return errorsLog;
    }

    /**
     * Конструктор на основе соединения к БД 
     */
    public GenericWorkPoolService(Connection connection, ErrorsLogService errorsLog) {
        this.connection = connection;
        this.errorsLog = errorsLog;
    }
    
    /**
     * @return the connection
     */
    protected Connection getConnection() {
        return connection;
    }

    private PreparedStatement clearWorkPoolDataStatement;

    private PreparedStatement lastOperationsStatement;
    @Override
    public PreparedStatement getLastOperationsStatement() throws SQLException {
        if (lastOperationsStatement == null) {
            lastOperationsStatement = 
                    getConnection().prepareStatement("SELECT MIN(id_superlog) AS id_superlog_min, MAX(id_superlog) AS id_superlog_max, id_foreign, id_table, COUNT(*) AS records_count, ? AS id_runner " +
                            "  FROM ( " +
                            "  SELECT id_superlog, id_foreign, id_table " +
                            "    FROM rep2_workpool_data " +
                            "    WHERE id_runner=? " +
                            "    ORDER BY id_superlog " +
                            "    LIMIT ? OFFSET ? " +
                            "  ) AS part_rep2_workpool_data " +
                            "GROUP BY id_foreign, id_table " +
                            "ORDER BY id_superlog_max",
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
        statement.setInt(2, runnerId);
        // Извлекаем частями равными fetchSize 
        statement.setFetchSize(fetchSize);
        
        // По задаче #2327
        // Задаем первоначальное смещение выборки равное 0.
        // При появлении ошибочных записей будем его увеличивать на 1.
        statement.setInt(3, fetchSize);
        statement.setInt(4, offset);
        
        return statement.executeQuery();
    }

    @Override
    public PreparedStatement getClearWorkPoolDataStatement() throws SQLException {
        if (clearWorkPoolDataStatement == null) {
            clearWorkPoolDataStatement = 
                    getConnection().prepareStatement("DELETE FROM rep2_workpool_data WHERE id_runner=? AND id_foreign=? AND id_table=? AND id_superlog>=? AND id_superlog<=?");
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
        PreparedStatement deleteWorkPoolData = getClearWorkPoolDataStatement();
        deleteWorkPoolData.setInt(1, getRunner(operationsResult));
        deleteWorkPoolData.setLong(2, getForeign(operationsResult));
        deleteWorkPoolData.setString(3, getTable(operationsResult));
        deleteWorkPoolData.setLong(4, getSuperlogMin(operationsResult));
        deleteWorkPoolData.setLong(5, getSuperlogMax(operationsResult));
        deleteWorkPoolData.addBatch();
        getErrorsLog().setStatus(getRunner(operationsResult), getTable(operationsResult), getForeign(operationsResult), 1);
    }
    
    /**
     * Функция записи информации об ошибке
     * @throws SQLException 
     */
    public void trackError(String message, SQLException e, ResultSet operation) 
            throws SQLException{
        getErrorsLog().add(getRunner(operation), getTable(operation), getForeign(operation), message, e);
    }
    
    @Override
    public String getTable(ResultSet resultSet) throws SQLException {
        return resultSet.getString(ID_TABLE);
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
    public Long getSuperlogMax(ResultSet resultSet) throws SQLException {
        return resultSet.getLong(ID_SUPERLOG_MAX);
    }

    @Override
    public Long getSuperlogMin(ResultSet resultSet) throws SQLException {
        return resultSet.getLong(ID_SUPERLOG_MIN);
    }

    @Override
    public void close() throws SQLException {
        close(clearWorkPoolDataStatement);
        close(lastOperationsStatement);
    }
    
    /**
     * Закрыть PreparedStatement
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

    @Override
    public int getRecordsCount(ResultSet resultSet) throws SQLException {
        return resultSet.getInt(RECORDS_COUNT);
    } 
}
