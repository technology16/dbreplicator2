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

import javax.sql.DataSource;

import ru.taximaxim.dbreplicator2.el.ErrorsLogService;
import ru.taximaxim.dbreplicator2.el.FatalReplicationException;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.data.DataServiceSkeleton;

/**
 * @author volodin_aa
 *
 */
public class GenericWorkPoolService extends DataServiceSkeleton
        implements WorkPoolService, AutoCloseable {

    private final ErrorsLogService errorsLog;

    private PreparedStatement clearWorkPoolDataStatement;

    private PreparedStatement lastOperationsStatement;

    /**
     * Конструктор на основе соединения к БД
     */
    public GenericWorkPoolService(DataSource dataSource, ErrorsLogService errorsLog) {
        super(dataSource);
        this.errorsLog = errorsLog;
    }

    /**
     * @return the errorsLog
     */
    protected ErrorsLogService getErrorsLog() {
        return errorsLog;
    }

    @Override
    public PreparedStatement getLastOperationsStatement() throws FatalReplicationException {
        if (lastOperationsStatement == null) {
            // Сортируем записи rep2_workpool_data в порядке поступления
            try {
                lastOperationsStatement = getConnection().prepareStatement(
                        "SELECT MIN(id_superlog) AS id_superlog_min, MAX(id_superlog) AS id_superlog_max, id_foreign, id_table, COUNT(*) AS records_count, ? AS id_runner "
                                + "  FROM (SELECT id_superlog, id_foreign, id_table "
                                + "    FROM rep2_workpool_data WHERE id_runner=? "
                                + "    ORDER BY id_superlog LIMIT ? OFFSET ? "
                                + "  ) AS part_rep2_workpool_data "
                                + "GROUP BY id_foreign, id_table "
                                + "ORDER BY id_superlog_min",
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
    public ResultSet getLastOperations(int runnerId, int fetchSize, int offset) throws FatalReplicationException {
        PreparedStatement statement = getLastOperationsStatement();

        try {
            statement.setInt(1, runnerId);
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка при установки идентификатора раннера [id_runner = %d] в запрос получения последних операций", runnerId), e);
        }

        try {
            statement.setInt(2, runnerId);
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка при установки идентификатора раннера [id_runner = %d] в условие запроса получения последних операций", runnerId), e);
        }
        try {
            // Извлекаем частями равными fetchSize
            statement.setFetchSize(fetchSize);
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка при установки размера выборки [fetchSize = %d] в запрос получения последних операций", fetchSize), e);
        }

            // По задаче #2327
            // Задаем первоначальное смещение выборки равное 0.
            // При появлении ошибочных записей будем его увеличивать на 1.
        try {
            statement.setInt(3, fetchSize);
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка при установки размера выборки [LIMIT = %d] в запрос получения последних операций", fetchSize), e);
        }

        try {
            statement.setInt(4, offset);
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка при установки смещения выборки [OFFSET = %d] в запрос получения последних операций", fetchSize), e);
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
                        "DELETE FROM rep2_workpool_data WHERE id_runner=? AND id_foreign=? AND id_table=? AND id_superlog>=? AND id_superlog<=?");
            } catch (SQLException e) {
                throw new FatalReplicationException(
                        "Ошибка при получении подготовленного запроса для удаления обработанных данных из рабочего набора", e);
            }
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
    @Override
    public void clearWorkPoolData(ResultSet operationsResult) throws FatalReplicationException {
        // Очищаем данные о текущей записи из набора данных реплики
        PreparedStatement deleteWorkPoolData = getClearWorkPoolDataStatement();
        try {
            deleteWorkPoolData.setInt(1, getRunner(operationsResult));
            deleteWorkPoolData.setLong(2, getForeign(operationsResult));
            deleteWorkPoolData.setString(3, getTable(operationsResult));
            deleteWorkPoolData.setLong(4, getSuperlogMin(operationsResult));
            deleteWorkPoolData.setLong(5, getSuperlogMax(operationsResult));
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

        getErrorsLog().setStatus(getRunner(operationsResult), getTable(operationsResult), getForeign(operationsResult),
                1);
    }

    /**
     * Функция записи информации об ошибке
     * 
     * @throws SQLException
     */
    @Override
    public void trackError(String message, SQLException e, ResultSet operation)
            throws FatalReplicationException {
            getErrorsLog().add(getRunner(operation), getTable(operation),
                    getForeign(operation), message, e);
    }

    @Override
    public String getTable(ResultSet resultSet) throws FatalReplicationException {
        try {
            return resultSet.getString(ID_TABLE);
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка при получении названия таблицы из поля [%s]", ID_TABLE), e);
        }
    }

    @Override
    public Long getForeign(ResultSet resultSet) throws FatalReplicationException {
        try {
            return resultSet.getLong(ID_FOREIGN);
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка при получении идентификатора записи из поля [%s]", ID_FOREIGN), e);
        }
    }

    @Override
    public int getRunner(ResultSet resultSet) throws FatalReplicationException {
        try {
            return resultSet.getInt(ID_RUNNER);
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка при получении идентификатора раннера из поля [%s]", ID_RUNNER), e);
        }
    }

    @Override
    public Long getSuperlogMax(ResultSet resultSet) throws FatalReplicationException {
        try {
            return resultSet.getLong(ID_SUPERLOG_MAX);
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка при получении максимального идентификатора записи в супер логе из поля [%s]", ID_SUPERLOG_MAX), e);
        }
    }

    @Override
    public Long getSuperlogMin(ResultSet resultSet) throws FatalReplicationException {
        try {
            return resultSet.getLong(ID_SUPERLOG_MIN);
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка при получении минимального идентификатора записи в супер логе из поля [%s]", ID_SUPERLOG_MAX), e);
        }
    }

    @Override
    public void close() throws FatalReplicationException {
        try (PreparedStatement thisClearWorkPoolDataStatement = this.clearWorkPoolDataStatement;
                PreparedStatement thisLastOperationsStatement = this.lastOperationsStatement) {
            super.close();
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    "Ошибка при закрытии ресурсов", e);
        }
    }

    @Override
    public int getRecordsCount(ResultSet resultSet) throws FatalReplicationException {
        try {
            return resultSet.getInt(RECORDS_COUNT);
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка при получении количества записей из поля [%s]", ID_SUPERLOG_MAX), e);
        }
    }
}
