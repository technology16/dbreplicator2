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
package ru.taximaxim.dbreplicator2.replica.strategies.replication;

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
import ru.taximaxim.dbreplicator2.replica.DataService;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;
import ru.taximaxim.dbreplicator2.replica.WorkPoolService;

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
    GenericAlgorithm(int fetchSize, int batchSize, boolean isStrict, 
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

    protected int replicateInsertion(Connection sourceConnection, 
            Connection targetConnection,
            WorkPoolService workPoolService, 
            ResultSet operationsResult,
            TableModel table,
            ResultSet sourceResult) throws SQLException {
        PreparedStatement insertDestStatement = 
                destDataService.getInsertStatement(table);
        // Добавляем данные в целевую таблицу
        Jdbc.fillStatementFromResultSet(insertDestStatement,
                sourceResult, 
                new ArrayList<String>(destDataService.getAllCols(table)));
        return insertDestStatement.executeUpdate();
    }

    protected int replicateUpdation(Connection sourceConnection, 
            Connection targetConnection,
            WorkPoolService workPoolService, 
            ResultSet operationsResult,
            TableModel table,
            ResultSet sourceResult) throws SQLException {
        // Если Была операция вставки или изменения, то сначала пытаемся обновить запись,
        PreparedStatement updateDestStatement = 
                destDataService.getUpdateStatement(table);
        // Добавляем данные в целевую таблицу
        List<String> colsForUpdate = new ArrayList<String>(destDataService.getDataCols(table));
        colsForUpdate.addAll(destDataService.getPriCols(table));
        Jdbc.fillStatementFromResultSet(updateDestStatement,
                sourceResult, colsForUpdate);
        return updateDestStatement.executeUpdate();
    }

    protected int replicateDeletion(Connection sourceConnection, 
            Connection targetConnection,
            WorkPoolService workPoolService, 
            ResultSet operationsResult,
            TableModel table) throws SQLException{
        // Если была операция удаления, то удаляем запись в приемнике
        PreparedStatement deleteDestStatement = 
                destDataService.getDeleteStatement(table);
        deleteDestStatement.setLong(1, operationsResult.getLong("id_foreign"));
        return deleteDestStatement.executeUpdate();
    }
    
    protected void replicateOperation(Connection sourceConnection, 
            Connection targetConnection, 
            StrategyModel data, 
            WorkPoolService workPoolService, 
            ResultSet operationsResult) throws SQLException{
        TableModel table = data.getRunner().getSource()
                .getTable(operationsResult.getString("id_table"));
        // Реплицируем данные
        if (operationsResult.getString("c_operation").equalsIgnoreCase("D")) {
            try {
                replicateDeletion(sourceConnection, 
                        targetConnection,
                        workPoolService, 
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
                    LOG.debug("Поглощена ошибка при удалении записи: " + rowDump, e);
                } else {
                    LOG.warn("Поглощена ошибка при удалении записи: " + rowDump + " " + e.getMessage());
                }
                workPoolService.trackError("Ошибка при удалении записи: " + rowDump, e, operationsResult);
                
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
                    int updationCount = -1;
                    // -1   - была ошибка при обновлении
                    // 0    - запись отсутствует в приемнике
                    // 1    - запись обновлена
                    try {
                        updationCount = replicateUpdation(sourceConnection, 
                                targetConnection,
                                workPoolService, 
                                operationsResult,
                                table,
                                sourceResult);
                    } catch (SQLException e) {
                        // Поглощаем и логгируем ошибки обновления
                        // Это ожидаемый результат
                        String rowDump = String.format("[ tableName = %s  [ operation = %s  [ row = %s ] ] ]", 
                                table, operationsResult.getString("c_operation"), 
                                Jdbc.resultSetToString(sourceResult, 
                                        new ArrayList<String>(sourceDataService.getAllCols(table))));
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Поглощена ошибка при обновлении записи: " + rowDump, e);
                        } else {
                            LOG.warn("Поглощена ошибка при обновлении записи: " + rowDump + " " + e.getMessage());
                        }
                        workPoolService.trackError("Ошибка при обновлении записи: " + rowDump, e, operationsResult);

                        if (isStrict) {
                            throw e;
                        }
                    }
                    if (updationCount > 0) {
                        workPoolService.clearWorkPoolData(operationsResult);
                    } else if (updationCount == 0) {
                        try {
                            // и если такой записи нет, то пытаемся вставить
                            replicateInsertion(sourceConnection, 
                                    targetConnection,
                                    workPoolService, 
                                    operationsResult,
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
                                LOG.debug("Поглощена ошибка при вставке записи: " + rowDump, e);
                            } else {
                                LOG.warn("Поглощена ошибка при вставке записи: " + rowDump + " " + e.getMessage());
                            }
                            workPoolService.trackError("Ошибка при вставке записи: " + rowDump, e, operationsResult);

                            if (isStrict) {
                                throw e;
                            }
                        }
                    }
                }
            }
        }
    }

    protected void selectLastOperations(Connection sourceConnection, 
            Connection targetConnection, StrategyModel data, 
            WorkPoolService workPoolService) throws SQLException{
        // Извлекаем список последних операций по измененым записям
        try {
            PreparedStatement selectLastOperations = 
                    workPoolService.getLastOperationsStatement(data.getRunner().getId(), fetchSize);
            PreparedStatement deleteWorkPoolData = 
                    workPoolService.getClearWorkPoolDataStatement();

            ResultSet operationsResult = selectLastOperations.executeQuery();
            try {
                // Проходим по списку измененных записей
                for (int rowsCount = 1; operationsResult.next(); rowsCount++) {
                    // Реплицируем операцию
                    replicateOperation(sourceConnection, targetConnection, 
                            data, workPoolService, operationsResult);

                    // Периодически сбрасываем батч в БД
                    if ((rowsCount % batchSize) == 0) {
                        deleteWorkPoolData.executeBatch();
                        sourceConnection.commit();

                        // Извлекаем новую порцию данных
                        operationsResult.close();
                        operationsResult = selectLastOperations.executeQuery();

                        LOG.info(String.format("Обработано %s строк...", rowsCount));
                    }
                }
            } finally {
                operationsResult.close();
            }
            // Подтверждаем транзакцию
            deleteWorkPoolData.executeBatch();
            sourceConnection.commit();
        } catch (SQLException e) {
            sourceConnection.rollback();
            throw e;
        }
    }

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

            selectLastOperations(sourceConnection, 
                    targetConnection, data, 
                    workPoolService);        
        } catch (SQLException e) {
            try {
                if (lastAutoCommit != null) {
                    sourceConnection.setAutoCommit(lastAutoCommit);
                }
            } catch(SQLException sqlException){
                // Ошибка может возникнуть если во время операции упало соединение к БД
                LOG.warn("Ошибка при возврате автокомита в исходное состояние.", sqlException);
            }

            try {
                if (lastTargetAutoCommit != null) {
                    targetConnection.setAutoCommit(lastTargetAutoCommit);
                }
            } catch(SQLException sqlException){
                // Ошибка может возникнуть если во время операции упало соединение к БД
                LOG.warn("Ошибка при возврате автокомита в исходное состояние.", sqlException);
            }
            throw e;
        }
    }
    
}