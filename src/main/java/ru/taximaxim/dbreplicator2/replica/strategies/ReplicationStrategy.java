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

package ru.taximaxim.dbreplicator2.replica.strategies;

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
import ru.taximaxim.dbreplicator2.replica.strategies.replication.Skeleton;

/**
 * Класс стратегии репликации данных из источника в приемник
 * Записи реплицируются в порядке последних операций над ними.
 * 
 * @author volodin_aa
 * 
 */
public class ReplicationStrategy extends Skeleton implements Strategy {

    private static final Logger LOG = Logger.getLogger(ReplicationStrategy.class);

    /**
     * Конструктор по умолчанию
     */
    public ReplicationStrategy() {
        super();
    }

    @Override
    public void execute(Connection sourceConnection, Connection targetConnection,
            StrategyModel data) throws StrategyException {
        // TODO: Реализовать поддержку списка таблиц
        Boolean lastAutoCommit = null;
        Boolean lastTargetAutoCommit = null;
        try {
            lastAutoCommit = sourceConnection.getAutoCommit();
            lastTargetAutoCommit = targetConnection.getAutoCommit();
            // Начинаем транзакцию
            sourceConnection.setAutoCommit(false);
            sourceConnection
            .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            sourceConnection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);

            targetConnection.setAutoCommit(true);
            // Извлекаем список последних операций по измененым записям
            try (
                    PreparedStatement selectLastOperations = 
                    sourceConnection.prepareStatement("SELECT * FROM rep2_workpool_data WHERE id_superlog IN (SELECT MAX(id_superlog) FROM rep2_workpool_data WHERE id_runner=? GROUP BY id_foreign, id_table)");
                    PreparedStatement deleteWorkPoolData = 
                            sourceConnection.prepareStatement("DELETE FROM rep2_workpool_data WHERE id_runner=? AND id_foreign=? AND id_table=? AND id_superlog<=?");
                    ) {
                selectLastOperations.setInt(1, data.getId());
                try (ResultSet operationsResult = selectLastOperations.executeQuery();) {
                    // Проходим по списку измененных записей
                    while (operationsResult.next()) {
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
                                    String rows = Jdbc.resultSetToString(sourceResult, colsForDelete);
                                    LOG.error(String.format(
                                            "Поглощена ошибка при удалении записи: [ tableName = %s  [ operation = %s  [ row   = %s ] ] ]", 
                                            tableName, operationsResult.getString("c_operation"), rows));
                                    trackError("Ошибка при удалении записи: ", e, sourceConnection, operationsResult);
                                }
                            }
                        } else {
                            // Добавляем данные в целевую таблицу
                            // Извлекаем данные из исходной таблицы
                            List<String> colsList = JdbcMetadata
                                    .getColumnsList(sourceConnection, tableName);
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
                                                            LOG.warn("Поглощена ошибка при вставке записи: ", e);
                                                            trackError("Ошибка при вставке записи: ", e, sourceConnection, operationsResult);
                                                        }
                                                    }
                                                } else {
                                                    clearWorkPoolData(deleteWorkPoolData, operationsResult);
                                                }
                                            } catch (SQLException e) {
                                                // Поглощаем и логгируем ошибки обновления
                                                // Это ожидаемый результат
                                                LOG.warn("Поглощена ошибка при обновлении записи: ", e);
                                                trackError("Ошибка при обновлении записи: ", e, sourceConnection, operationsResult);
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                // Подтверждаем транзакцию
                deleteWorkPoolData.executeBatch();
                sourceConnection.commit();
            } catch (Exception e) {
                sourceConnection.rollback();
                throw new StrategyException(e);
            }
        } catch (Exception e) {
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
            throw new StrategyException(e);
        }
    }

}