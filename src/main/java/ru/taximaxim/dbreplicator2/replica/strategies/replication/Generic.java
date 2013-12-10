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
import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.jdbc.QueryConstructors;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;

/**
 * Класс стратегии репликации данных из источника в приемник
 * Записи реплицируются в порядке последних операций над ними.
 * 
 * @author volodin_aa
 * 
 */
public class Generic extends Skeleton implements Strategy {

    private static final Logger LOG = Logger.getLogger(Generic.class);

    /**
     * Размер выборки данных (строк)
     */
    private int fetchSize = 1000;
    
    /**
     * Размер сбрасываемых в БД данных (строк)
     */
    private int batchSize = 1000;

    /**
     * Конструктор по умолчанию
     */
    public Generic() {
        super();
    }

    @Override
    public void execute(Connection sourceConnection, Connection targetConnection,
            StrategyModel data) throws StrategyException, SQLException {
        // TODO: Реализовать поддержку списка таблиц
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
            // Извлекаем список последних операций по измененым записям
            try (
                    PreparedStatement selectLastOperations = 
                            sourceConnection.prepareStatement("SELECT * FROM rep2_workpool_data WHERE id_superlog IN (SELECT MAX(id_superlog) AS id_superlog FROM rep2_workpool_data WHERE id_runner=? GROUP BY id_foreign, id_table ORDER BY id_superlog) ORDER BY id_superlog", 
                                    ResultSet.TYPE_FORWARD_ONLY,
                                    ResultSet.CONCUR_READ_ONLY);
                    PreparedStatement deleteWorkPoolData = 
                            sourceConnection.prepareStatement("DELETE FROM rep2_workpool_data WHERE id_runner=? AND id_foreign=? AND id_table=? AND id_superlog<=?");
                    ) {
                selectLastOperations.setInt(1, data.getId());
                // Извлекаем частями равными fetchSize 
                selectLastOperations.setFetchSize(fetchSize);
                
                ResultSet operationsResult = selectLastOperations.executeQuery();
                try {
                    // Проходим по списку измененных записей
                    for (int rowsCount = 1; operationsResult.next(); rowsCount++) {
                        String tableName = operationsResult.getString("id_table");
                        List<String> priColsList = JdbcMetadata.getPrimaryColumnsList(sourceConnection, tableName);
                        // Реплицируем данные
                        if (operationsResult.getString("c_operation").equalsIgnoreCase("D")) {
                            // Если была операция удаления, то удаляем запись в приемнике
                            String deleteDestQuery = QueryConstructors
                                    .constructDeleteQuery(tableName, priColsList);
                            try (PreparedStatement deleteDestStatement = targetConnection
                                    .prepareStatement(deleteDestQuery);
                                    ) {
                                deleteDestStatement.setLong(1, operationsResult.getLong("id_foreign"));
                                try {
                                    deleteDestStatement.executeUpdate();
                                    clearWorkPoolData(deleteWorkPoolData, operationsResult);
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
                                    trackError("Ошибка при удалении записи: " + rowDump, e, sourceConnection, operationsResult);
                                }
                            }
                        } else {
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
                                            try {
                                                if (updateDestStatement.executeUpdate()<1) {
                                                    // и если такой записи нет, то пытаемся вставить
                                                    String insertDestQuery = QueryConstructors
                                                            .constructInsertQuery(tableName, colsList);
                                                    try (
                                                            PreparedStatement insertDestStatement = targetConnection
                                                            .prepareStatement(insertDestQuery);
                                                            ) {
                                                        // Добавляем данные в целевую таблицу
                                                        Jdbc.fillStatementFromResultSet(insertDestStatement,
                                                                sourceResult, colsList);
                                                        try {
                                                            insertDestStatement.executeUpdate();
                                                            clearWorkPoolData(deleteWorkPoolData, operationsResult);
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
                                                            trackError("Ошибка при вставке записи: " + rowDump, e, sourceConnection, operationsResult);
                                                        }
                                                    }
                                                } else {
                                                    clearWorkPoolData(deleteWorkPoolData, operationsResult);
                                                }
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
                                                trackError("Ошибка при обновлении записи: " + rowDump, e, sourceConnection, operationsResult);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        
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