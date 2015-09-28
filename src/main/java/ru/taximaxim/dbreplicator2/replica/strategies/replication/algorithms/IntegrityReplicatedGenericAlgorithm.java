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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.jdbc.Jdbc;
import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.model.TableModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.data.GenericDataTypeService;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.workpool.WorkPoolService;
/**
 * Заготовка стратегии репликации
 * 
 * @author volodin_aa
 *
 */
public class IntegrityReplicatedGenericAlgorithm extends GenericAlgorithm implements Strategy {

    private static final String INTEGRITY_ERROR = "Ошибка в целостности реплицированных данных [%s => %s]\n";

    private static final Logger LOG = Logger.getLogger(IntegrityReplicatedGenericAlgorithm.class);
    
    private static final String ID_RUNNER = "idRunner";
    
    private GenericDataTypeService sourceDataService;
    private GenericDataTypeService destDataService;
    
    /**
     * Конструктор по умолчанию
     * 
     * @param fetchSize - размер выборки за раз данных
     * @param isStrict
     * @param workPoolService
     * @param sourceDataService
     * @param destDataService
     */
    public IntegrityReplicatedGenericAlgorithm(int fetchSize, 
            WorkPoolService workPoolService,
            GenericDataTypeService sourceDataService, GenericDataTypeService destDataService) {
        super(fetchSize, false, workPoolService, sourceDataService, destDataService);
        this.sourceDataService = sourceDataService;
        this.destDataService = destDataService;
    }

    /**
     * @return the sourceDataService
     */
    protected GenericDataTypeService getSourceDataService() {
        return sourceDataService;
    }

    /**
     * @return the destDataService
     */
    protected GenericDataTypeService getDestDataService() {
        return destDataService;
    }
    
    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.strategies.replication.algorithms.GenericAlgorithm#replicateRecord(ru.taximaxim.dbreplicator2.model.StrategyModel, java.sql.ResultSet, ru.taximaxim.dbreplicator2.model.TableModel, ru.taximaxim.dbreplicator2.model.TableModel, java.sql.ResultSet)
     */
    @Override
    protected boolean replicateRecord(StrategyModel data, ResultSet operationsResult,
            TableModel sourceTable, TableModel destTable, ResultSet sourceResult)
            throws SQLException {
        // Вместо репликации проверяем запись
        StringBuffer rowDumpHead = new StringBuffer(String.format(
                INTEGRITY_ERROR, data
                        .getRunner().getSource().getPoolId(), data.getRunner()
                        .getTarget().getPoolId()));
        List<String> priCols = new ArrayList<String>(getSourceDataService().getPriCols(
                sourceTable));

        PreparedStatement selectTargetStatement = getDestDataService()
                .getSelectStatement(destTable);
        selectTargetStatement.setLong(1, getWorkPoolService()
                .getForeign(operationsResult));

        try (ResultSet targetResult = selectTargetStatement.executeQuery();) {
            if (targetResult.next()) {
                Map<String, Integer> colsSource = new HashMap<String, Integer>(
                        getSourceDataService().getAllColsTypes(sourceTable));

                boolean errorRows = false;
                for (Entry<String, Integer> column : colsSource.entrySet()) {
                    String colsName = column.getKey();
                    if (!JdbcMetadata.isEquals(sourceResult, targetResult, colsName,
                            column.getValue())) {
                        String rowDump = String.format("[ поле %s => [%s != %s] ] ",
                                colsName, sourceResult.getObject(colsName),
                                targetResult.getObject(colsName));
                        rowDumpHead.append(rowDump);
                        errorRows = true;
                    }
                }
                if (errorRows) {
                    rowDumpHead.insert(0, String.format(
                            "Ошибка в таблицах %s -> %s, данные не равны в строке %s: ",
                            sourceTable.getName(), destTable.getName(),
                            Jdbc.resultSetToString(sourceResult, priCols)));
                    getWorkPoolService().trackError(rowDumpHead.toString(),
                            new SQLException(), operationsResult);

                    return false;
                } else {
                    getWorkPoolService().clearWorkPoolData(operationsResult);
                    return true;
                }
            }
        }

        rowDumpHead.append(String.format(
                "Ошибка в таблице %s, отсутствует запись приемнике %s",
                destTable.getName(), Jdbc.resultSetToString(sourceResult, priCols)));
        getWorkPoolService().trackError(rowDumpHead.toString(), new SQLException(),
                operationsResult);
        return false;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.strategies.replication.algorithms.GenericAlgorithm#deleteRecord(ru.taximaxim.dbreplicator2.model.StrategyModel, java.sql.ResultSet, ru.taximaxim.dbreplicator2.model.TableModel, ru.taximaxim.dbreplicator2.model.TableModel)
     */
    @Override
    protected boolean deleteRecord(StrategyModel data, ResultSet operationsResult,
            TableModel sourceTable, TableModel destTable) throws SQLException {
        // Вместо удаления проверяем отсутствие записи в приемнике
        PreparedStatement selectTargetStatement = getDestDataService()
                .getSelectStatement(destTable);
        selectTargetStatement.setLong(1, getWorkPoolService().getForeign(operationsResult));
        try (ResultSet targetResult = selectTargetStatement.executeQuery();) {            
            if(targetResult.next()) {
                StringBuffer rowDumpHead = new StringBuffer(String.format(
                        INTEGRITY_ERROR, data
                                .getRunner().getSource().getPoolId(), data.getRunner()
                                .getTarget().getPoolId()));
                List<String> priCols = new ArrayList<String>(getSourceDataService().getPriCols(
                        sourceTable));
                
                rowDumpHead.append(String.format(
                        "Ошибка в таблице %s, присутствует удаленная запись приемнике %s",
                        destTable.getName(),
                        Jdbc.resultSetToString(targetResult, priCols)));
                    getWorkPoolService().trackError(rowDumpHead.toString(), new SQLException(), operationsResult);
                    return false;
            }
        }
        getWorkPoolService().clearWorkPoolData(operationsResult);
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
    @Override
    protected void selectLastOperations(Connection sourceConnection, 
            Connection targetConnection, StrategyModel data) throws SQLException, ClassNotFoundException {
        int runnerId = Integer.parseInt(data.getParam(ID_RUNNER));
        // Задаем первоначальное смещение выборки равное 0.
        // При появлении ошибочных записей будем его увеличивать на 1.
        int offset = 0;
        // Извлекаем список последних операций по измененым записям
        PreparedStatement deleteWorkPoolData = 
                getWorkPoolService().getClearWorkPoolDataStatement();
        ResultSet operationsResult = 
                getWorkPoolService().getLastOperations(runnerId, getFetchSize(), offset);
        try {
            // Проходим по списку измененных записей
            for (int rowsCount = 1; operationsResult.next(); rowsCount++) {
                // Реплицируем операцию
                if (!replicateOperation(data, operationsResult)) {
                    offset++;
                }

                // Периодически сбрасываем батч в БД
                if ((rowsCount % getFetchSize()) == 0) {
                    deleteWorkPoolData.executeBatch();

                    // Извлекаем новую порцию данных
                    operationsResult.close();
                    operationsResult = getWorkPoolService().getLastOperations(runnerId, getFetchSize(), offset);

                    LOG.info(String.format("Раннер [id_runner = %s, %s] Стратегия [id = %s]: Обработано %s строк...", 
                            data.getRunner().getId(), data.getRunner().getDescription(), data.getId(), rowsCount));
                }
            }
        } finally {
            operationsResult.close();
        }
        // Подтверждаем транзакцию
        deleteWorkPoolData.executeBatch();
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.strategies.replication.algorithms.GenericAlgorithm#getSourceTable(ru.taximaxim.dbreplicator2.model.StrategyModel, java.sql.ResultSet)
     */
    @Override
    protected TableModel getSourceTable(StrategyModel data, ResultSet operationsResult)
            throws SQLException {
        return data.getRunner().getSource()
                .getRunner(Integer.parseInt(data.getParam(ID_RUNNER)))
                .getTable(getWorkPoolService().getTable(operationsResult));
    }

    /**
     * Получение сопоставленной таблицы в приемке для таблицы источника
     * 
     * @param data
     * @param sourceTable
     * @return
     */
    @Override
    protected TableModel getDestTable(StrategyModel data, TableModel sourceTable) {
        TableModel destTable = destTables.get(sourceTable);
        if (destTable == null) {
            destTable = sourceTable;
            // Проверяем, есть ли явное сопоставление имен таблиц
            String destTableName = data.getRunner().getSource()
                    .getRunner(Integer.parseInt(data.getParam(ID_RUNNER)))
                    .getTable(sourceTable.getName()).getParam("dest");
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
}
