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
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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
    
    protected static final String NEW_LINE = " \n";
    
    private static final int DEFAULT_FETCH_SIZE = 1000;
    
    /**
     * Размер выборки данных (строк)
     */
    private int fetchSize = DEFAULT_FETCH_SIZE;

    private boolean isStrict = false;

    private WorkPoolService workPoolService;

    private DataService sourceDataService;

    private DataService destDataService;

    private Count countSuccess;
    private Count countError;
    
    protected Map<TableModel, TableModel> destTables = new HashMap<TableModel, TableModel>();
    
    /**
     * Перечисление возможных типов ошибок при репликации
     * @author petrov_im
     *
     */
    protected enum ErrorType {
        INSERT,
        UPDATE,
        UPDATE_INSERT,
        DELETE,
        EXTRACT_DEST,
        EXTRACT_SOURCE;
        
        /**
         * Текст сообщения в зависимости от типа ошибки
         * @return
         */
        public String getMessageBody() {
            switch(this) {
            case INSERT:
                return "Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка при вставке записи: \n[ tableName = %s  [ row = %s ] ]";
            case UPDATE:
                return "Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка при обновлении записи: \n[ tableName = %s  [ row = %s ] ]";
            case UPDATE_INSERT:
                return "Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка при вставке/обновлении записи: \n[ tableName = %s  [ row = %s ] ]";
            case DELETE:
                return "Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка при удалении записи: \n[ tableName = %s  [ row = [ id = %s ] ] ]";
            case EXTRACT_DEST:
                return "Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка извлечения данных из приемника: \n[ tableName = %s  [ row = %s ] ]";
            case EXTRACT_SOURCE:
                return "Раннер [id_runner = %s, %s] Стратегия [id = %s]: Поглощена ошибка извлечения данных из источника: \n[ tableName = %s  [ row = [ id = %s ] ] ]";
            default:
                return "Неизвестный тип ошибки!";
            }
        }
    }
    
    /**
     * Конструктор нпо умолчанию
     * 
     * @param fetchSize - размер выборки данных
     * @param isStrict - флаг строго режима репликации. Если true, то 
     * репликации останавливается при первой же ошибки.
     */
    public GenericAlgorithm(int fetchSize, boolean isStrict, 
            WorkPoolService workPoolService,
            DataService sourceDataService,
            DataService destDataService) {
        this.fetchSize = fetchSize;
        this.isStrict = isStrict;
        this.workPoolService = workPoolService;
        this.sourceDataService = sourceDataService;
        this.destDataService = destDataService;
        countSuccess = new Count();
        countError = new Count();
    }

    /**
     * Счетчик успешных операций
     * @return
     */
    protected Count getCountSuccess() {
        return countSuccess;
    }
    
    /**
     * Счетчик ошибочных оперраций
     * @return
     */
    protected Count getCountError() {
        return countError;
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
     * @param destTable - модель таблицы источника
     * @param data  - текущая запись из источника.
     * 
     * @return количество измененых записей
     * 
     * @throws SQLException
     */
    protected int replicateInsertion(TableModel sourceTable, TableModel destTable,
            ResultSet data) throws SQLException {
        PreparedStatement insertDestStatement = 
                getDestDataService().getInsertStatement(
                        destTable, 
                        getSourceDataService().getAllCols(sourceTable));
        // Добавляем данные в целевую таблицу
        Jdbc.fillStatementFromResultSet(insertDestStatement,
                data, 
                getDestDataService().getAllAvaliableCols(destTable, 
                        getSourceDataService().getAllCols(sourceTable)));
        return insertDestStatement.executeUpdate();
    }

    /**
     * Функция репликации обновления записи.
     * 
     * @param destTable - модель таблицы источника
     * @param data  - текущая запись из источника.
     * 
     * @return количество измененых записей
     * 
     * @throws SQLException
     */
    protected int replicateUpdation(TableModel sourceTable, TableModel destTable,
            ResultSet data) throws SQLException {
        // Если Была операция вставки или изменения, то сначала пытаемся обновить запись,
        PreparedStatement updateDestStatement = 
                getDestDataService().getUpdateStatement(
                        destTable, 
                        getSourceDataService().getAllCols(sourceTable));
        // Добавляем данные в целевую таблицу
        Collection<String> colsForUpdate = 
                new ArrayList<String>(
                        getDestDataService().getAvaliableDataCols(destTable, 
                                getSourceDataService().getAllCols(sourceTable)));
        colsForUpdate.addAll(getDestDataService().getPriCols(destTable));
        Jdbc.fillStatementFromResultSet(updateDestStatement,
                data, colsForUpdate);
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
        deleteDestStatement.setLong(1, getWorkPoolService().getForeign(operationsResult));
        return deleteDestStatement.executeUpdate();
    }

    /**
     * Получение сопоставленной таблицы в приемке для таблицы источника
     * 
     * @param data
     * @param sourceTable
     * @return
     */
    protected TableModel getDestTable(StrategyModel data, TableModel sourceTable) {
        TableModel destTable = destTables.get(sourceTable);
        if (destTable == null) {
            destTable = sourceTable;
            // Проверяем, есть ли явное сопоставление имен таблиц
            String destTableName = data.getRunner().getTable(sourceTable.getName()).getParam("dest");
            if (destTableName != null) {
                // Создаем копию для таблицы приемника
                destTable = (TableModel) sourceTable.clone();
                destTable.setName(destTableName);
                destTable.setParam("tempKey", "tempValue");
                destTable.setRunner(null);
            }
            destTables.put(sourceTable, destTable);
        }
        return destTable;
    }

    /**
     * Обработка ситуации, когда в списке таблиц раннера отсутствует
     * таблица полученная из воркпула
     * (ситуация возможна при рестарте репликатора с непустым воркпулом)
     * @param data
     * @param operationsResult
     * @throws SQLException
     */
    protected void tableIsNullHandling(StrategyModel data, ResultSet operationsResult) throws SQLException {
        String message = String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: В списке таблиц раннера отстутсвует таблица %s", 
                data.getRunner().getId(), 
                data.getRunner().getDescription(), 
                data.getId(),
                operationsResult.getString(4)); 
        SQLException e = new SQLException(message);
        addErrorLog(message, e, operationsResult);
    }
    
    /**
     * Обработка ошибок репликации
     * @param data
     * @param operationsResult
     * @throws SQLException
     */
    protected void errorsHandling(ErrorType eType, StrategyModel data, TableModel sourceTable,
            ResultSet sourceResult, ResultSet operationsResult, SQLException e) throws SQLException {
        
        String rowMess;
        if (eType == ErrorType.DELETE || eType == ErrorType.EXTRACT_SOURCE) {
            rowMess = String.valueOf(getWorkPoolService().getForeign(operationsResult));
        } else {
            rowMess = Jdbc.resultSetToString(sourceResult, 
                    getSourceDataService().getAllCols(sourceTable));
        }
        String message = String.format(eType.getMessageBody(), 
                data.getRunner().getId(), 
                data.getRunner().getDescription(), 
                data.getId(),
                getDestTable(data, sourceTable).getName(),
                rowMess);
        addErrorLog(message, e, operationsResult);
    }
    
    protected void addErrorLog(String message, SQLException e, ResultSet operationsResult) throws SQLException {
        if (LOG.isDebugEnabled()) {
            LOG.debug(message, e);
        } else {
            LOG.warn(message + NEW_LINE + e.getMessage());
        }
        getWorkPoolService().trackError(message, e, operationsResult);
        getCountError().add(getWorkPoolService().getTable(operationsResult));
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
        TableModel sourceTable = data.getRunner().getTable(getWorkPoolService().getTable(operationsResult));
        if (sourceTable == null) {
            // Обработка возможной ошибки при рестарте
            // с непустым воркпулом
            tableIsNullHandling(data, operationsResult);
            return false;
        }
        TableModel destTable = getDestTable(data, sourceTable);
        
        // Извлекаем данные из исходной таблицы
        PreparedStatement selectSourceStatement = 
                getSourceDataService().getSelectStatement(
                        sourceTable);
        selectSourceStatement.setLong(1, getWorkPoolService().getForeign(operationsResult));
        try (ResultSet sourceResult = selectSourceStatement.executeQuery();) {
            if (sourceResult.next()) {
                // Извлекаем данные из приемника
                PreparedStatement selectDestStatement = 
                        getDestDataService().getSelectStatement(
                                destTable);
                selectDestStatement.setLong(1, getWorkPoolService().getForeign(operationsResult));
                try (ResultSet destResult = selectDestStatement.executeQuery();) {
                    if (destResult.next()) {
                        // Добавляем данные в целевую таблицу
                        // 0    - запись отсутствует в приемнике
                        // 1    - запись обновлена
                        // Пробуем обновить запись
                        try {
                            replicateUpdation(sourceTable, destTable, sourceResult);
                            getWorkPoolService().clearWorkPoolData(operationsResult);
                            getCountSuccess().add(getWorkPoolService().getTable(operationsResult));
                            
                            return true;
                        } catch (SQLException e) {
                            // Поглощаем и логгируем ошибки обновления
                            errorsHandling(ErrorType.UPDATE, data, sourceTable, sourceResult, operationsResult, e);
                            
                            return false;
                        }
                    } else {
                        try {
                            // и если такой записи нет, то пытаемся вставить
                            replicateInsertion(sourceTable, destTable, sourceResult);
                            getWorkPoolService().clearWorkPoolData(operationsResult);
                            getCountSuccess().add(getWorkPoolService().getTable(operationsResult));
                            
                            return true;
                        } catch (SQLException e) {
                            // Поглощаем и логгируем ошибки вставки
                            errorsHandling(ErrorType.INSERT, data, sourceTable, sourceResult, operationsResult, e);
                            
                            return false;
                        }
                    }
                } catch (SQLException e) {
                    // Поглощаем и логгируем ошибки извлечения данных из приемника
                    errorsHandling(ErrorType.EXTRACT_DEST, data, sourceTable, sourceResult, operationsResult, e);
                    
                    return false;
                }
            } else {
                // Если запись фактически отсутствует, то
                // удалем ее в приемнике
                try {
                    replicateDeletion(operationsResult, destTable);
                    getWorkPoolService().clearWorkPoolData(operationsResult);
                    getCountSuccess().add(getWorkPoolService().getTable(operationsResult));
                    
                    return true;
                } catch (SQLException e) {
                    // Поглощаем и логгируем ошибки удаления
                    errorsHandling(ErrorType.DELETE, data, sourceTable, null, operationsResult, e);
                    
                    return false;
                }
            }
        } catch (SQLException e) {
            // Поглощаем и логгируем ошибки извлечения данных из источника
            errorsHandling(ErrorType.EXTRACT_SOURCE, data, sourceTable, null, operationsResult, e);
            
            return false;
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
        boolean fetchNext;
        do {
            fetchNext = false;
            
            try (ResultSet operationsResult = getWorkPoolService().getLastOperations(
                    data.getRunner().getId(), getFetchSize(), offset);) {
                int succed = 0;
                int all = 0;
                // Проходим по списку измененных записей
                while (operationsResult.next()) {
                    // Реплицируем операцию
                    if (replicateOperation(data, operationsResult)) {
                        succed = succed + getWorkPoolService().getRecordsCount(operationsResult);
                    } else {
                        if (isStrict()) {
                            // Устанавливаем флаг выборки новой порции данных только 
                            // в случае успешного цикла обработки данных, иначе 
                            // прерываем обработку данных в strict режиме
                            fetchNext = false;
                            break;
                        } else {
                            // Пропускаем сгруппированые записи
                            offset = offset + getWorkPoolService().getRecordsCount(operationsResult);
                        }
                    } 
                    all = all + getWorkPoolService().getRecordsCount(operationsResult);
                    
                    fetchNext = true;
                }
                // Если реплицирована хотя бы 1 одна запись и в выборке было меньше 
                // fetchSize, и были ошибки, то начинаем реплицировать с начала воркпула
                if ((0<succed) && (all<getFetchSize()) && (offset>0)) {
                    offset = 0;
                }
                getWorkPoolService().getClearWorkPoolDataStatement().executeBatch();
            } finally {
                writeStatCount(data.getId());
            }
        } while (fetchNext);
    }
    
    /**
     * Запись счетчиков
     * @param strategy
     * @throws SQLException
     * @throws ClassNotFoundException 
     */
    protected void writeStatCount(int strategy) throws SQLException, ClassNotFoundException{
        Timestamp date = new Timestamp(new Date().getTime());

        for (String tableName : getCountSuccess().getTables()) {
            getStatsService().writeStat(date, StatsService.TYPE_SUCCESS, strategy, tableName, 
                    getCountSuccess().getCount(tableName));
        }
        getCountSuccess().clear();
        
        for (String tableName : getCountError().getTables()) {
            getStatsService().writeStat(date, StatsService.TYPE_ERROR, strategy, tableName, 
                    getCountError().getCount(tableName));
        }
        getCountError().clear();
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
            sourceConnection.setAutoCommit(true);
            sourceConnection
            .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

            targetConnection.setAutoCommit(true);

            // Устанавливаем флаг текущего владельца записей
            getSourceDataService().setRepServerName(data.getRunner().getTarget().getPoolId());
            getDestDataService().setRepServerName(data.getRunner().getSource().getPoolId());

            selectLastOperations(sourceConnection, targetConnection, data);
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
