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
package ru.taximaxim.dbreplicator2.replica.strategies.replication.algorithms;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.jdbc.Jdbc;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.model.TableModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.data.DataService;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.workpool.WorkPoolService;

/**
 * Заготовка стратегии репликации
 * 
 * @author volodin_aa
 *
 */
public class GenericAlgorithm implements Strategy {

    private static final Logger LOG = Logger.getLogger(GenericAlgorithm.class);

    /**
     * Размер выборки данных (строк)
     */
    private int fetchSize = 1000;

    /**
     * Размер сбрасываемых в БД данных (строк)
     */
    private int batchSize = 1000;

    private boolean isStrict = false;
    
    private WorkPoolService workPoolService;
    
    private DataService sourceDataService;
    
    private DataService destDataService;

    /**
     * 
     * @param fetchSize
     * @param batchSize
     * @param isStrict
     */
    public GenericAlgorithm(int fetchSize, int batchSize, boolean isStrict, 
            WorkPoolService workPoolService,
            DataService sourceDataService,
            DataService destDataService) {
        this.fetchSize = fetchSize;
        this.batchSize = batchSize;
        this.isStrict = isStrict;
        this.workPoolService = workPoolService;
        this.sourceDataService = sourceDataService;
        this.destDataService = destDataService;
    }

    /**
     * @return the fetchSize
     */
    protected int getFetchSize() {
        return fetchSize;
    }

    /**
     * @return the batchSize
     */
    protected int getBatchSize() {
        return batchSize;
    }

    /**
     * @return the workPoolService
     */
    protected WorkPoolService getWorkPoolService() {
        return workPoolService;
    }

    /**
     * @return the sourceDataService
     */
    protected DataService getSourceDataService() {
        return sourceDataService;
    }

    /**
     * @return the destDataService
     */
    protected DataService getDestDataService() {
        return destDataService;
    }

    /**
     * @return the isStrict
     */
    protected boolean isStrict() {
        return isStrict;
    }

    /**
     * Функция репликации вставки записи.
     * 
     * @param dataService   - сервис для работы с приемником
     * @param table         - модель таблицы источника
     * @param sourceResult  - текущая запись из источника.
     * @return количество измененых записей
     * @throws SQLException
     */
    protected int replicateInsertion(DataService dataService,
            TableModel table,
            ResultSet sourceResult) throws SQLException {
        PreparedStatement insertDestStatement = 
                dataService.getInsertStatement(table);
        // Добавляем данные в целевую таблицу
        Jdbc.fillStatementFromResultSet(insertDestStatement,
                sourceResult, 
                new ArrayList<String>(dataService.getAllCols(table)));
        return insertDestStatement.executeUpdate();
    }

    /**
     * Функция репликации обновления записи.
     * 
     * @param dataService   - сервис для работы с приемником
     * @param table         - модель таблицы источника
     * @param sourceResult  - текущая запись из источника.
     * @return количество измененых записей
     * @throws SQLException
     */
    protected int replicateUpdation(DataService dataService,
            TableModel table,
            ResultSet sourceResult) throws SQLException {
        // Если Была операция вставки или изменения, то сначала пытаемся обновить запись,
        PreparedStatement updateDestStatement = 
                dataService.getUpdateStatement(table);
        // Добавляем данные в целевую таблицу
        List<String> colsForUpdate = new ArrayList<String>(dataService.getDataCols(table));
        colsForUpdate.addAll(dataService.getPriCols(table));
        Jdbc.fillStatementFromResultSet(updateDestStatement,
                sourceResult, colsForUpdate);
        return updateDestStatement.executeUpdate();
    }

    /**
     * Функция репликации удаления записи.
     * 
     * @param dataService       - сервис для работы с приемником
     * @param operationsResult  - текущая запись из очереди операций.
     * @param table             - модель таблицы источника
     * @return количество измененых записей
     * @throws SQLException
     */
    protected int replicateDeletion(DataService dataService, 
            ResultSet operationsResult,
            TableModel table) throws SQLException{
        // Если была операция удаления, то удаляем запись в приемнике
        PreparedStatement deleteDestStatement = 
                dataService.getDeleteStatement(table);
        deleteDestStatement.setLong(1, operationsResult.getLong("id_foreign"));
        return deleteDestStatement.executeUpdate();
    }
    
    /**
     * Функция для репликации данных. Здесь вызываются подфункции репликации 
     * конкретных операций и обрабатываются исключительнык ситуации.
     * 
     * @param data              - настройки стратегии
     * @param workPoolService   - сервис для работы с очередью опереций
     * @param sourceDataService - сервис для работы с источником
     * @param destDataService   - сервис для работы с приемником
     * @param operationsResult  - текущая запись из очереди операций.
     * @throws SQLException
     */
    protected void replicateOperation(StrategyModel data, 
            WorkPoolService workPoolService,
            DataService sourceDataService,
            DataService destDataService, 
            ResultSet operationsResult,
            boolean isStrict) throws SQLException{
        TableModel table = data.getRunner().getSource()
                .getTable(operationsResult.getString("id_table"));
        // Реплицируем данные
        if (operationsResult.getString("c_operation").equalsIgnoreCase("D")) {
            try {
                replicateDeletion(destDataService, 
                        operationsResult,
                        table);
                workPoolService.clearWorkPoolData(operationsResult);
            } catch (SQLException e) {
                // Поглощаем и логгируем ошибки удаления
                // Это ожидаемый результат
                String rowDump = String.format(
                        "[ tableName = %s  [ operation = D  [ row = [ id = %s ] ] ] ]", 
                        table, String.valueOf(operationsResult.getLong("id_foreign")));
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка при удалении записи: ", 
                            data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) + rowDump, e);
                } else {
                    LOG.warn(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка при удалении записи: ", 
                            data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) + rowDump + " " + e.getMessage());
                }
                workPoolService.trackError(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Ошибка при удалении записи: ", 
                        data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) + rowDump, e, operationsResult);
                
                if (isStrict) {
                    throw e;
                }
            }
        } else {
            // Добавляем данные в целевую таблицу
            // Извлекаем данные из исходной таблицы
            PreparedStatement selectSourceStatement = 
                    sourceDataService.getSelectStatement(table);
            selectSourceStatement.setLong(1, operationsResult.getLong("id_foreign"));
            try (ResultSet sourceResult = selectSourceStatement.executeQuery();) {
                // Проходим по списку измененных записей
                if (sourceResult.next()) {
                    int updationCount = 0;
                    boolean hasError = false;
                    // 0    - запись отсутствует в приемнике
                    // 1    - запись обновлена
                    try {
                        updationCount = replicateUpdation(destDataService,
                                table,
                                sourceResult);
                    } catch (SQLException e) {
                        hasError = true;
                        // Поглощаем и логгируем ошибки обновления
                        // Это ожидаемый результат
                        String rowDump = String.format("[ tableName = %s  [ operation = %s  [ row = %s ] ] ]", 
                                table, operationsResult.getString("c_operation"), 
                                Jdbc.resultSetToString(sourceResult, 
                                        new ArrayList<String>(sourceDataService.getAllCols(table))));
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка при обновлении записи: ", 
                                    data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) + rowDump, e);
                        } else {
                            LOG.warn(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка при обновлении записи: ", 
                                    data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) 
                                    + rowDump + " " + e.getMessage());
                        }
                        workPoolService.trackError(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Ошибка при обновлении записи: ", 
                                data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) + rowDump, e, operationsResult);

                        if (isStrict) {
                            throw e;
                        }
                    }
                    if (!hasError) {
                        if (updationCount > 0) {
                            workPoolService.clearWorkPoolData(operationsResult);
                        } else {
                            try {
                                // и если такой записи нет, то пытаемся вставить
                                replicateInsertion(destDataService,
                                        table,
                                        sourceResult);
                                workPoolService.clearWorkPoolData(operationsResult);
                            } catch (SQLException e) {
                                // Поглощаем и логгируем ошибки вставки
                                // Это ожидаемый результат
                                String rowDump = String.format("[ tableName = %s  [ operation = %s  [ row = %s ] ] ]", 
                                        table, operationsResult.getString("c_operation"), 
                                        Jdbc.resultSetToString(sourceResult, 
                                                new ArrayList<String>(sourceDataService.getAllCols(table))));
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка при вставке записи: ", 
                                            data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) + rowDump, e);
                                } else {
                                    LOG.warn(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка при вставке записи: ", 
                                            data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) + rowDump + " " + e.getMessage());
                                }
                                workPoolService.trackError(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Ошибка при вставке записи: ", 
                                        data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) + rowDump, e, operationsResult);

                                if (isStrict) {
                                    throw e;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Функция отбора обрабатываемых операций из очереди операций.
     * Для каждой операции вызывается функция replicateOperation(...).
     * 
     * 
     * @param sourceConnection  - соединение к источнику данных
     * @param targetConnection  - целевое соединение
     * @param data              - данные стратегии
     * @param workPoolService   - сервис для работы с очередью опереций
     * @param sourceDataService - сервис для работы с источником
     * @param destDataService   - сервис для работы с приемником
     * @throws SQLException
     */
    protected void selectLastOperations(Connection sourceConnection, 
            Connection targetConnection, StrategyModel data, 
            WorkPoolService workPoolService,
            DataService sourceDataService,
            DataService destDataService,
            int fetchSize, int batchSize, boolean isStrict) throws SQLException {
        // Задаем первоначальное смещение выборки равное 0.
        // При появлении ошибочных записей будем его увеличивать на 1.
        int offset = 0;
        // Извлекаем список последних операций по измененым записям
        PreparedStatement deleteWorkPoolData = 
                workPoolService.getClearWorkPoolDataStatement();
        ResultSet operationsResult = 
                workPoolService.getLastOperations(data.getRunner().getId(), fetchSize, offset);
        try {
            // Проходим по списку измененных записей
            for (int rowsCount = 1; operationsResult.next(); rowsCount++) {
                // Реплицируем операцию
                replicateOperation(data, workPoolService,
                        sourceDataService,
                        destDataService, 
                        operationsResult, isStrict);

                // Периодически сбрасываем батч в БД
                if ((rowsCount % batchSize) == 0) {
                    deleteWorkPoolData.executeBatch();
                    sourceConnection.commit();

                    // Извлекаем новую порцию данных
                    operationsResult.close();
                    operationsResult = workPoolService.getLastOperations(data.getRunner().getId(), fetchSize, offset);

                    LOG.info(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Обработано %s строк...", 
                            data.getRunner().getId(), data.getRunner().getDescription(), data.getId(), rowsCount));
                }
            }
        } finally {
            operationsResult.close();
        }
        // Подтверждаем транзакцию
        deleteWorkPoolData.executeBatch();
        sourceConnection.commit();
    }

    /**
     * Точка входа в алгоритм репликации.
     * Здесь настраивается режим работы соединений к БД и вызывается функция
     * отбора операций selectLastOperations(...).
     */
    public void execute(Connection sourceConnection, Connection targetConnection,
            StrategyModel data) throws StrategyException, SQLException {
        Boolean lastAutoCommit = null;
        Boolean lastTargetAutoCommit = null;
        try {
            lastAutoCommit = sourceConnection.getAutoCommit();
            lastTargetAutoCommit = targetConnection.getAutoCommit();
            // Начинаем транзакцию
            sourceConnection.setAutoCommit(false);
            sourceConnection
            .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            targetConnection.setAutoCommit(true);

            // Устанавливаем флаг текущего владельца записей
            getSourceDataService().setRepServerName(data.getRunner().getTarget().getPoolId());
            getDestDataService().setRepServerName(data.getRunner().getSource().getPoolId());
            
            selectLastOperations(sourceConnection, 
                    targetConnection, data, 
                    getWorkPoolService(),
                    getSourceDataService(),
                    getDestDataService(),
                    getFetchSize(),
                    getBatchSize(),
                    isStrict());
            
            sourceConnection.commit();
        } catch (SQLException e) {
            sourceConnection.rollback();
            throw e;
        } finally {    
            // Сбрасываем флаг текущего владельца записей
            getSourceDataService().setRepServerName(null);
            getDestDataService().setRepServerName(null);
            
            try {
                if (lastAutoCommit != null) {
                    sourceConnection.setAutoCommit(lastAutoCommit);
                }
            } catch(SQLException sqlException){
                // Ошибка может возникнуть если во время операции упало соединение к БД
                LOG.warn(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Ошибка при возврате автокомита в исходное состояние.", 
                        data.getRunner().getId(), data.getRunner().getDescription(), data.getId()), sqlException);
            }

            try {
                if (lastTargetAutoCommit != null) {
                    targetConnection.setAutoCommit(lastTargetAutoCommit);
                }
            } catch(SQLException sqlException){
                // Ошибка может возникнуть если во время операции упало соединение к БД
                LOG.warn(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Ошибка при возврате автокомита в исходное состояние.", 
                        data.getRunner().getId(), data.getRunner().getDescription(), data.getId()), sqlException);
            }
        }
    }
    
}