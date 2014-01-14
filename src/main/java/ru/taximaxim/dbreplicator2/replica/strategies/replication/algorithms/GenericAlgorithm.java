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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.jdbc.Jdbc;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.model.TableModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.data.DataService;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.workpool.WorkPoolService;
import ru.taximaxim.dbreplicator2.stats.StatsService;
import ru.taximaxim.dbreplicator2.utils.Core;
import ru.taximaxim.dbreplicator2.utils.Count;

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
    
    private Count count;
    
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
        count = new Count();
    }

    protected Count getCount() {
        return count;
    }
    
    /**
     * @return StatsService
     */
    protected StatsService getStatsService() {
        return Core.getStatsService();
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
     * @param table         - модель таблицы источника
     * @param sourceResult  - текущая запись из источника.
     * 
     * @return количество измененых записей
     * 
     * @throws SQLException
     */
    protected int replicateInsertion(TableModel table,
            ResultSet sourceResult) throws SQLException {
        PreparedStatement insertDestStatement = 
                getDestDataService().getInsertStatement(table);
        // Добавляем данные в целевую таблицу
        Jdbc.fillStatementFromResultSet(insertDestStatement,
                sourceResult, 
                new ArrayList<String>(getDestDataService().getAllCols(table)));
        return insertDestStatement.executeUpdate();
    }

    /**
     * Функция репликации обновления записи.
     * 
     * @param table         - модель таблицы источника
     * @param sourceResult  - текущая запись из источника.
     * 
     * @return количество измененых записей
     * 
     * @throws SQLException
     */
    protected int replicateUpdation(TableModel table,
            ResultSet sourceResult) throws SQLException {
        // Если Была операция вставки или изменения, то сначала пытаемся обновить запись,
        PreparedStatement updateDestStatement = 
                getDestDataService().getUpdateStatement(table);
        // Добавляем данные в целевую таблицу
        List<String> colsForUpdate = new ArrayList<String>(getDestDataService().getDataCols(table));
        colsForUpdate.addAll(getDestDataService().getPriCols(table));
        Jdbc.fillStatementFromResultSet(updateDestStatement,
                sourceResult, colsForUpdate);
        return updateDestStatement.executeUpdate();
    }

    /**
     * Функция репликации удаления записи.
     * 
     * @param operationsResult  - текущая запись из очереди операций.
     * @param table             - модель таблицы источника
     * 
     * @return количество измененых записей
     * 
     * @throws SQLException
     */
    protected int replicateDeletion(ResultSet operationsResult,
            TableModel table) throws SQLException{
        // Если была операция удаления, то удаляем запись в приемнике
        PreparedStatement deleteDestStatement = 
                getDestDataService().getDeleteStatement(table);
        deleteDestStatement.setLong(1, operationsResult.getLong("id_foreign"));
        return deleteDestStatement.executeUpdate();
    }
    
    /**
     * Функция для репликации данных. Здесь вызываются подфункции репликации 
     * конкретных операций и обрабатываются исключительнык ситуации.
     * 
     * @param data              - настройки стратегии
     * @param operationsResult  - текущая запись из очереди операций.
     * 
     * @return true если данные реплицировались без ошибок
     * 
     * @throws SQLException
     */
    protected boolean replicateOperation(StrategyModel data, 
            ResultSet operationsResult) throws SQLException{
        TableModel table = data.getRunner().getSource()
                .getTable(operationsResult.getString("id_table"));
        // Реплицируем данные
        if (operationsResult.getString("c_operation").equalsIgnoreCase("D")) {
            try {
                replicateDeletion(operationsResult, table);
                getWorkPoolService().clearWorkPoolData(operationsResult);
                getCount().addSuccess(table.getName());
            } catch (SQLException e) {
                // Поглощаем и логгируем ошибки удаления
                getCount().addError(table.getName());
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
                getWorkPoolService().trackError(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Ошибка при удалении записи: ", 
                        data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) + rowDump, e, operationsResult);

                return false;
            }
        } else {
            // Добавляем данные в целевую таблицу
            // Извлекаем данные из исходной таблицы
            PreparedStatement selectSourceStatement = 
                    getSourceDataService().getSelectStatement(table);
            selectSourceStatement.setLong(1, operationsResult.getLong("id_foreign"));
            try (ResultSet sourceResult = selectSourceStatement.executeQuery();) {
                // Проходим по списку измененных записей
                if (sourceResult.next()) {
                    // 0    - запись отсутствует в приемнике
                    // 1    - запись обновлена
                    try {
                        if (replicateUpdation(table, sourceResult) > 0) {
                            getWorkPoolService().clearWorkPoolData(operationsResult);
                            getCount().addSuccess(table.getName());
                        } else {
                            try {
                                // и если такой записи нет, то пытаемся вставить
                                replicateInsertion(table, sourceResult);
                                getCount().addSuccess(table.getName());
                                
                                getWorkPoolService().clearWorkPoolData(operationsResult);
                            } catch (SQLException e) {
                                // Поглощаем и логгируем ошибки вставки
                                getCount().addError(table.getName());
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
                                getWorkPoolService().trackError(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Ошибка при вставке записи: ", 
                                        data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) + rowDump, e, operationsResult);

                                return false;
                            }
                        }
                    } catch (SQLException e) {
                        // Поглощаем и логгируем ошибки обновления
                        getCount().addError(table.getName());
                        // Это ожидаемый результат
                        String rowDump = String.format("[ tableName = %s  [ operation = %s  [ row = %s ] ] ]", 
                                table, operationsResult.getString("c_operation"), 
                                Jdbc.resultSetToString(sourceResult, 
                                        new ArrayList<String>(getSourceDataService().getAllCols(table))));
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка при обновлении записи: ", 
                                    data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) + rowDump, e);
                        } else {
                            LOG.warn(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка при обновлении записи: ", 
                                    data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) 
                                    + rowDump + " " + e.getMessage());
                        }
                        getWorkPoolService().trackError(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Ошибка при обновлении записи: ", 
                                data.getRunner().getId(), data.getRunner().getDescription(), data.getId()) + rowDump, e, operationsResult);

                        return false;
                    }
                }
            }
        }
        
        return true;
    }

    /**
     * Функция отбора обрабатываемых операций из очереди операций.
     * Для каждой операции вызывается функция replicateOperation(...).
     * 
     * 
     * @param sourceConnection  - соединение к источнику данных
     * @param targetConnection  - целевое соединение
     * @param data              - данные стратегии
     * 
     * @throws SQLException
     * @throws ClassNotFoundException 
     */
    protected void selectLastOperations(Connection sourceConnection, 
            Connection targetConnection, StrategyModel data) throws SQLException, ClassNotFoundException {
        // Задаем первоначальное смещение выборки равное 0.
        // При появлении ошибочных записей будем его увеличивать на 1.
        int offset = 0;
        // Извлекаем список последних операций по измененым записям
        PreparedStatement deleteWorkPoolData = 
                getWorkPoolService().getClearWorkPoolDataStatement();
        ResultSet operationsResult = 
                getWorkPoolService().getLastOperations(data.getRunner().getId(), getFetchSize(), offset);
        try {
            // Проходим по списку измененных записей
            for (int rowsCount = 1; operationsResult.next(); rowsCount++) {
                // Реплицируем операцию
                
                if (!replicateOperation(data, operationsResult)) {
                    if (isStrict()) {
                        break;
                    } else {
                        offset++;
                    }
                }

                // Периодически сбрасываем батч в БД
                if ((rowsCount % getBatchSize()) == 0) {
                    deleteWorkPoolData.executeBatch();
                    sourceConnection.commit();

                    // Извлекаем новую порцию данных
                    operationsResult.close();
                    operationsResult = getWorkPoolService().getLastOperations(data.getRunner().getId(), getFetchSize(), offset);

                    LOG.info(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Обработано %s строк...", 
                            data.getRunner().getId(), data.getRunner().getDescription(), data.getId(), rowsCount));
                }
            }
        } finally {
            operationsResult.close();
            writeStatСount(data.getId());
        }
        // Подтверждаем транзакцию
        deleteWorkPoolData.executeBatch();
        sourceConnection.commit();
    }

    /**
     * Запись счетчиков
     * @param strategy
     * @throws SQLException
     * @throws ClassNotFoundException 
     */
    protected void writeStatСount(int strategy) throws SQLException, ClassNotFoundException{
        Timestamp date = new Timestamp(new Date().getTime());
        
        for (String tableName : getCount().getSuccessTables()) {
             getStatsService().writeStat(date, 1, strategy, tableName, 
                  getCount().getSuccess(tableName));
        }
        
        for (String tableName : getCount().getErrorTables()) {
             getStatsService().writeStat(date, 0, strategy, tableName, 
                 getCount().getError(tableName));
        }
    }
    
    /**
     * Точка входа в алгоритм репликации.
     * Здесь настраивается режим работы соединений к БД и вызывается функция
     * отбора операций selectLastOperations(...).
     * @throws ClassNotFoundException 
     */
    public void execute(Connection sourceConnection, Connection targetConnection,
            StrategyModel data) throws StrategyException, SQLException, ClassNotFoundException {
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
            
            selectLastOperations(sourceConnection, targetConnection, data);
            
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