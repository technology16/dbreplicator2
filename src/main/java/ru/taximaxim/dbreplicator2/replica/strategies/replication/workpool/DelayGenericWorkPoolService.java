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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

import javax.sql.DataSource;

import ru.taximaxim.dbreplicator2.el.ErrorsLogService;
import ru.taximaxim.dbreplicator2.el.FatalReplicationException;

/**
 * @author volodin_aa
 *
 */
public class DelayGenericWorkPoolService extends GenericWorkPoolService
        implements WorkPoolService, AutoCloseable {

    private PreparedStatement lastOperationsStatement;

    private final int period;

    private PreparedStatement clearWorkPoolDataStatement;

    /**
     * Конструктор
     * 
     * @param connection
     * @param errorsLog
     */
    public DelayGenericWorkPoolService(DataSource dataSource, ErrorsLogService errorsLog,
            int period) {
        super(dataSource, errorsLog);
        this.period = period;
    }

    /**
     * Получение периода
     * 
     * @return
     */
    public int getPeriod() {
        return period;
    }

    @Override
    public PreparedStatement getLastOperationsStatement() throws FatalReplicationException {
        if (lastOperationsStatement == null) {
            try {
                lastOperationsStatement = getConnection().prepareStatement(
                        "SELECT DISTINCT id_runner, id_foreign, id_table FROM rep2_workpool_data WHERE id_runner=? and c_date<=? LIMIT ? OFFSET ?",
                        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            } catch (SQLException e) {
                throw new FatalReplicationException(
                        "Ошибка при получении подготовленного выражения для выборки последних операций", e);
            }
        }

        return lastOperationsStatement;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * ru.taximaxim.dbreplicator2.replica.WorkPoolService#getLastOperations(int,
     * int, int)
     */
    @Override
    public ResultSet getLastOperations(int runnerId, int fetchSize, int offset)
            throws FatalReplicationException {
        PreparedStatement statement = getLastOperationsStatement();

        try {
            statement.setInt(1, runnerId);
    
            Timestamp date = new Timestamp(
                    Calendar.getInstance().getTimeInMillis() - getPeriod());
            statement.setTimestamp(2, date);
    
            // Извлекаем частями равными fetchSize
            statement.setFetchSize(fetchSize);
    
            // По задаче #2327
            // Задаем первоначальное смещение выборки равное 0.
            // При появлении ошибочных записей будем его увеличивать на 1.
            statement.setInt(3, fetchSize);
                statement.setInt(4, offset);
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    "Ошибка при подстановки параметров в подготовленное выражение для выборки последних операций", e);
        }

        try {
            return statement.executeQuery();
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    "Ошибка при получении последних операций", e);
        }
    }

    @Override
    public PreparedStatement getClearWorkPoolDataStatement() throws FatalReplicationException {
        if (clearWorkPoolDataStatement == null) {
            try {
                clearWorkPoolDataStatement = getConnection().prepareStatement(
                        "DELETE FROM rep2_workpool_data WHERE id_runner=? AND id_foreign=? AND id_table=?");
            } catch (SQLException e) {
                throw new FatalReplicationException(
                        "Ошибка при получении подготовленного запроса для удаления обработанных данных из рабочего набора", e);
            }
        }

        return clearWorkPoolDataStatement;
    }

    /*
     * (non-Javadoc)
     * 
     * @see ru.taximaxim.dbreplicator2.replica.strategies.replication.workpool.
     * GenericWorkPoolService#clearWorkPoolData(java.sql.ResultSet)
     */
    @Override
    public void clearWorkPoolData(ResultSet operationsResult) throws FatalReplicationException {
        // Очищаем данные о текущей записи из набора данных реплики
        PreparedStatement deleteWorkPoolData = getClearWorkPoolDataStatement();
        try {
        deleteWorkPoolData.setInt(1, getRunner(operationsResult));
        deleteWorkPoolData.setLong(2, getForeign(operationsResult));
        deleteWorkPoolData.setString(3, getTable(operationsResult));
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    "Ошибка при установки параметров подготовленного запроса для удаления обработанных данных из рабочего набора", e);
        }

        try {
        deleteWorkPoolData.addBatch();
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    "Ошибка при добавлениии в пакет запросов для удаления обработанных данных из рабочего набора", e);
        }

        getErrorsLog().setStatus(getRunner(operationsResult), getTable(operationsResult),
                getForeign(operationsResult), 1);
    }

    @Override
    public void close() throws FatalReplicationException {
        try (PreparedStatement lastOperationsStatement = this.lastOperationsStatement;
                PreparedStatement clearWorkPoolDataStatement = this.clearWorkPoolDataStatement;) {
            super.close();
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    "Ошибка при закрытии ресурсов", e);
        }
    }
}
