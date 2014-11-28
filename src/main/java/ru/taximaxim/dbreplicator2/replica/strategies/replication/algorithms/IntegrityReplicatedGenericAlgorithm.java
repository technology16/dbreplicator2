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
                    sourceConnection.commit();

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
        sourceConnection.commit();
    }
    
    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.strategies.replication.GenericAlgorithm#replicateOperation(ru.taximaxim.dbreplicator2.model.StrategyModel, ru.taximaxim.dbreplicator2.replica.strategies.replication.workpool.WorkPoolService, ru.taximaxim.dbreplicator2.replica.strategies.replication.data.DataService, ru.taximaxim.dbreplicator2.replica.strategies.replication.data.DataService, java.sql.ResultSet)
     */
    @Override
    protected boolean replicateOperation(StrategyModel data, ResultSet operationsResult) throws SQLException {
        boolean result = true;
        TableModel sourceTable = data.getRunner().getSource().getTable(getWorkPoolService().getTable(operationsResult));
        TableModel destTable = getDestTable(data, sourceTable);
        
        // Извлекаем данные из исходной таблицы
        PreparedStatement selectSourceStatement = getSourceDataService()
                .getSelectStatement(sourceTable,
                        getDestDataService().getAllCols(destTable));
        selectSourceStatement.setLong(1, getWorkPoolService()
                .getForeign(operationsResult));

        PreparedStatement selectTargetStatement = getDestDataService()
                .getSelectStatement(destTable,
                        getSourceDataService().getAllCols(sourceTable));

        try (ResultSet sourceResult = selectSourceStatement.executeQuery();) {
            StringBuffer rowDumpHead = new StringBuffer(String.format("Ошибка в целостности реплицированных данных [%s => %s]\n",
                    data.getRunner().getSource().getPoolId(),
                    data.getRunner().getTarget().getPoolId()));
            Map<String, Integer> colsSource = new HashMap<String, Integer>(getSourceDataService().getAllColsTypes(sourceTable));
            List<String> priCols = new ArrayList<String>(getSourceDataService().getPriCols(sourceTable));
            if(sourceResult.next()) {
                selectTargetStatement.setLong(1, getWorkPoolService().getForeign(operationsResult));
                
                try (ResultSet targetResult = selectTargetStatement.executeQuery();) {            
                    if(targetResult.next()) {
                        boolean errorRows = false;
                        for (Entry<String, Integer> column: colsSource.entrySet()) {
                            String colsName = column.getKey();
                            if(!JdbcMetadata.isEquals(sourceResult, targetResult, colsName, column.getValue())) {
                                String rowDump = String.format("[ поле %s => [%s != %s] ] ",  colsName, sourceResult.getObject(colsName), targetResult.getObject(colsName));
                                rowDumpHead.append(rowDump);
                                errorRows = true;
                             }
                        }
                        if(errorRows) {
                            result = false;
                            rowDumpHead.insert(0, String.format("Ошибка в таблицах %s -> %s, данные не равны в строке %s: ", 
                                    sourceTable.getName(),
                                    destTable.getName(),
                                    Jdbc.resultSetToString(sourceResult, priCols)));
                            getWorkPoolService().trackError(rowDumpHead.toString(), new SQLException(), operationsResult);
 
                        } else {
                            getWorkPoolService().clearWorkPoolData(operationsResult);
                        }
                    } else {
                        rowDumpHead.append(String.format(
                            "Ошибка в таблице %s, отсутствует запись приемнике %s",
                            destTable.getName(),
                            Jdbc.resultSetToString(sourceResult, priCols)));
                        getWorkPoolService().trackError(rowDumpHead.toString(), new SQLException(), operationsResult);
                        result = false;
                    }
                }
            } else {
                selectTargetStatement.setLong(1, getWorkPoolService().getForeign(operationsResult));
                try (ResultSet targetResult = selectTargetStatement.executeQuery();) {            
                    if(targetResult.next()) {
                        rowDumpHead.append(String.format(
                                "Ошибка в таблице %s, присутствует удаленная запись приемнике %s",
                                destTable.getName(),
                                Jdbc.resultSetToString(targetResult, priCols)));
                            getWorkPoolService().trackError(rowDumpHead.toString(), new SQLException(), operationsResult);
                            result = false;
                    } else {
                        getWorkPoolService().clearWorkPoolData(operationsResult);
                    }
                }
            }
        }
        return result;
    }
}
