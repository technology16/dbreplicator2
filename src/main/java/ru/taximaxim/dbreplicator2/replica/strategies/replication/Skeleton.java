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
import java.util.Map;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.jdbc.Jdbc;
import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.jdbc.QueryConstructors;
import ru.taximaxim.dbreplicator2.model.IgnoreColumnsTableModel;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.model.TableModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;
import ru.taximaxim.dbreplicator2.replica.WorkPoolService;

/**
 * Заготовка стратегии репликации
 * 
 * @author volodin_aa
 *
 */
public class Skeleton implements Strategy {

    private static final Logger LOG = Logger.getLogger(Skeleton.class);

    /**
     * Размер выборки данных (строк)
     */
    private int fetchSize = 1000;

    /**
     * Размер сбрасываемых в БД данных (строк)
     */
    private int batchSize = 1000;

    private boolean isStrict = false;
    
    WorkPoolService workPoolService;

    private Map<String, List<String>> ignoreCols = new TreeMap<String, List<String>>(String.CASE_INSENSITIVE_ORDER);

    /**
     * 
     * @param fetchSize
     * @param batchSize
     * @param isStrict
     */
    Skeleton(int fetchSize, int batchSize, boolean isStrict, WorkPoolService workPoolService) {
        this.fetchSize = fetchSize;
        this.batchSize = batchSize;
        this.isStrict = isStrict;
        this.workPoolService = workPoolService;
    }
    
    protected void setIgnoreCols(StrategyModel data) throws SQLException {
        List<String> cols;
        List<TableModel> tableList = data.getRunner().getSource().getTables();
        for (TableModel tableModel : tableList) {
            cols = new ArrayList<String>();
            for (IgnoreColumnsTableModel ignoreCol : tableModel.getIgnoreColumnsTable()) {
                cols.add(ignoreCol.getColumnName().toUpperCase());
            }
            ignoreCols.put(tableModel.getName(), cols);
        }
    }
    
    protected List<String> getIgnoreCols(String tableName) {
        if(ignoreCols.get(tableName) == null) {
            return new ArrayList<String>();
        }
        else {
            return ignoreCols.get(tableName);
        }
    }
    
    protected int replicateInsertion(Connection sourceConnection, 
            Connection targetConnection,
            WorkPoolService workPoolService, 
            ResultSet operationsResult,
            String tableName,
            List<String> colsList,
            ResultSet sourceResult) throws SQLException {
        String insertDestQuery = QueryConstructors
                .constructInsertQuery(tableName, colsList);
        try (
                PreparedStatement insertDestStatement = targetConnection
                .prepareStatement(insertDestQuery);
                ) {
            // Добавляем данные в целевую таблицу
            Jdbc.fillStatementFromResultSet(insertDestStatement,
                    sourceResult, colsList);
            return insertDestStatement.executeUpdate();
        }
    }

    protected int replicateUpdation(Connection sourceConnection, 
            Connection targetConnection,
            WorkPoolService workPoolService, 
            ResultSet operationsResult,
            String tableName,
            List<String> colsList,
            List<String> priColsList,
            ResultSet sourceResult) throws SQLException {
        // Если Была операция вставки или изменения, то сначала пытаемся обновить запись,
        String updateDestQuery = QueryConstructors
                .constructUpdateQuery(tableName, colsList, priColsList);
        try (
                PreparedStatement updateDestStatement = targetConnection
                .prepareStatement(updateDestQuery);
                ) {
            // Добавляем данные в целевую таблицу
            List<String> colsForUpdate = new ArrayList<String>(colsList);
            colsForUpdate.addAll(priColsList);
            Jdbc.fillStatementFromResultSet(updateDestStatement,
                    sourceResult, colsForUpdate);
            return updateDestStatement.executeUpdate();
        }
    }

    protected int replicateDeletion(Connection sourceConnection, 
            Connection targetConnection,
            WorkPoolService workPoolService, 
            ResultSet operationsResult,
            String tableName) throws SQLException{
        List<String> priColsList = 
                JdbcMetadata.getPrimaryColumnsList(sourceConnection, tableName);
        // Если была операция удаления, то удаляем запись в приемнике
        String deleteDestQuery = QueryConstructors
                .constructDeleteQuery(tableName, priColsList);
        try (PreparedStatement deleteDestStatement = targetConnection
                .prepareStatement(deleteDestQuery);
                ) {
            deleteDestStatement.setLong(1, operationsResult.getLong("id_foreign"));
            return deleteDestStatement.executeUpdate();
        }
    }

    protected void replicateOperation(Connection sourceConnection, 
            Connection targetConnection, 
            WorkPoolService workPoolService, 
            ResultSet operationsResult) throws SQLException{
        String tableName = operationsResult.getString("id_table");
        // Реплицируем данные
        if (operationsResult.getString("c_operation").equalsIgnoreCase("D")) {
            try {
                replicateDeletion(sourceConnection, 
                        targetConnection,
                        workPoolService, 
                        operationsResult,
                        tableName);
                workPoolService.clearWorkPoolData(operationsResult);
            } catch (SQLException e) {
                // Поглощаем и логгируем ошибки удаления
                // Это ожидаемый результат
                String rowDump = String.format(
                        "[ tableName = %s  [ operation = D  [ row = [ id = %s ] ] ] ]", 
                        tableName, String.valueOf(operationsResult.getLong("id_foreign")));
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
            List<String> priColsList = 
                    JdbcMetadata.getPrimaryColumnsList(sourceConnection, tableName);
            // Добавляем данные в целевую таблицу
            // Извлекаем данные из исходной таблицы
            List<String> colsList = JdbcMetadata
                    .getColumnsList(sourceConnection, tableName);
            // Удаляем игнорируемые колонки
            for (String colm : getIgnoreCols(tableName)) {
                colsList.remove(colm);
            }
            String selectSourceQuery = QueryConstructors
                    .constructSelectQuery(tableName, colsList, priColsList);
            try (
                    PreparedStatement selectSourceStatement = sourceConnection
                    .prepareStatement(selectSourceQuery);
                    ) {
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
                                    tableName,
                                    colsList,
                                    colsList,
                                    sourceResult);
                        } catch (SQLException e) {
                            // Поглощаем и логгируем ошибки обновления
                            // Это ожидаемый результат
                            String rowDump = String.format("[ tableName = %s  [ operation = %s  [ row = %s ] ] ]", 
                                    tableName, operationsResult.getString("c_operation"), 
                                    Jdbc.resultSetToString(sourceResult, colsList));
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
                                        tableName,
                                        colsList,
                                        sourceResult);
                                workPoolService.clearWorkPoolData(operationsResult);
                            } catch (SQLException e) {
                                // Поглощаем и логгируем ошибки вставки
                                // Это ожидаемый результат
                                String rowDump = String.format("[ tableName = %s  [ operation = %s  [ row = %s ] ] ]", 
                                        tableName, operationsResult.getString("c_operation"), 
                                        Jdbc.resultSetToString(sourceResult, colsList));
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
                            workPoolService, operationsResult);

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
            //игнорируемые колонки
            setIgnoreCols(data);

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