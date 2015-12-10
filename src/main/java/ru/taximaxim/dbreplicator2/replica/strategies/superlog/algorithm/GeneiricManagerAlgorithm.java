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

package ru.taximaxim.dbreplicator2.replica.strategies.superlog.algorithm;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.jdbc.Jdbc;
import ru.taximaxim.dbreplicator2.model.HikariCPSettingsModel;
import ru.taximaxim.dbreplicator2.model.Runner;
import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.model.TableModel;
import ru.taximaxim.dbreplicator2.model.TaskSettings;
import ru.taximaxim.dbreplicator2.model.TaskSettingsService;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.StrategySkeleton;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.workpool.WorkPoolService;
import ru.taximaxim.dbreplicator2.replica.strategies.superlog.data.SuperlogDataService;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Класс стратегии менеджера записей суперлог таблицы
 * 
 * @author volodin_aa
 * 
 */
public abstract class GeneiricManagerAlgorithm extends StrategySkeleton implements Strategy {

    private static final Logger LOG = Logger.getLogger(GeneiricManagerAlgorithm.class);

    protected SuperlogDataService superlogDataService;

    private Set<Runner> tRunners;

    /**
     * Конструктор по умолчанию
     */
    public GeneiricManagerAlgorithm(SuperlogDataService superlogDataService) {
        this.superlogDataService = superlogDataService;
    }

    @Override
    public void execute(Connection sourceConnection, Connection targetConnection, StrategyModel data) throws StrategyException, SQLException {
        int fetchSize = getFetchSize(data);
        Boolean lastAutoCommit = null;
        Boolean lastTargetAutoCommit = null;
        lastAutoCommit = sourceConnection.getAutoCommit();
        lastTargetAutoCommit = targetConnection.getAutoCommit();
        // Начинаем транзакцию
        sourceConnection.setAutoCommit(true);
        targetConnection.setAutoCommit(true);
        HikariCPSettingsModel sourcePool = data.getRunner().getSource();
        Map<String, Collection<RunnerModel>> tableObservers = getTableObservers(sourcePool);

        sourceConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        targetConnection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        // Строим список обработчиков реплик
        ResultSet superLogResult = null;
        // Переносим данные
        try (
                PreparedStatement insertRunnerData = superlogDataService.getInsertWorkpoolStatement();
                PreparedStatement deleteSuperLog = superlogDataService.getDeleteSuperlogStatement();
                PreparedStatement selectSuperLog = superlogDataService.getSelectSuperlogStatement();
                PreparedStatement initSelectSuperLog = superlogDataService.getInitSelectSuperlogStatement();
                ) {

            superLogResult = initSelectSuperLog.executeQuery();
            Collection<String> cols = new ArrayList<String>();
            cols.add(WorkPoolService.ID_SUPERLOG);
            cols.add(WorkPoolService.ID_POOL);
            cols.add(WorkPoolService.ID_FOREIGN);
            cols.add(WorkPoolService.ID_TABLE);
            cols.add(WorkPoolService.C_OPERATION);
            cols.add(WorkPoolService.C_DATE);
            cols.add(WorkPoolService.ID_TRANSACTION);
            Set<RunnerModel> runners = new HashSet<RunnerModel>();
            int rowsCount = 1;
            if (!superLogResult.next()) {
                selectSuperLog.setLong(1, 0);
            } else {
                do {
                    // Выводим данные из rep2_superlog_table
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(Jdbc.resultSetToString(superLogResult, cols));
                    }
                    // Копируем записи
                    String tableName = superLogResult.getString(WorkPoolService.ID_TABLE);
                    Collection<RunnerModel> observers = tableObservers.get(tableName);
                    if (observers != null) {
                        for (RunnerModel runner : observers) {
                            if (!superLogResult.getString(WorkPoolService.ID_POOL).equals(runner.getTarget().getPoolId())) {
                                insertRunnerData.setInt(1, runner.getId());
                                insertRunnerData.setLong(2, superLogResult.getLong(WorkPoolService.ID_SUPERLOG));
                                insertRunnerData.setInt(3, superLogResult.getInt(WorkPoolService.ID_FOREIGN));
                                insertRunnerData.setString(4, tableName);
                                insertRunnerData.setString(5, superLogResult.getString(WorkPoolService.C_OPERATION));
                                insertRunnerData.setTimestamp(6, superLogResult.getTimestamp(WorkPoolService.C_DATE));
                                insertRunnerData.setString(7, superLogResult.getString(WorkPoolService.ID_TRANSACTION));
                                insertRunnerData.addBatch();
                                // Выводим данные из rep2_superlog_table
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug("INSERT");
                                }
                                runners.add(runner);
                            }
                            // Удаляем исходную запись
                            deleteSuperLog.setLong(1, superLogResult.getLong(WorkPoolService.ID_SUPERLOG));
                            deleteSuperLog.addBatch();
                        }
                    }

                    // Периодически сбрасываем батч в БД
                    if ((rowsCount % fetchSize) == 0) {
                        insertRunnerData.executeBatch();
                        deleteSuperLog.executeBatch();
                        selectSuperLog.setLong(1, superLogResult.getLong(WorkPoolService.ID_SUPERLOG));
                        superLogResult = selectSuperLog.executeQuery();
                        // запускаем обработчики реплик
                        startRunners(runners);
                        runners.clear();
                        LOG.info(String.format("Обработано %s строк...", rowsCount));
                    }
                    rowsCount ++;
                } while (superLogResult.next());
                insertRunnerData.executeBatch();
                deleteSuperLog.executeBatch();
            }
        } finally {
            if (superLogResult != null) {
                superLogResult.close();
            }
        }

        // запускаем обработчики реплик
        Set<RunnerModel> runners = new HashSet<RunnerModel>();
        for (Collection<RunnerModel> observers : tableObservers.values()) {
            runners.addAll(observers);
        }
        startRunners(runners);

        try {
            if (lastAutoCommit != null) {
                sourceConnection.setAutoCommit(lastAutoCommit);
            }
        } catch (SQLException e) {
            // Ошибка может возникнуть если во время операции упало
            // соединение к БД
            LOG.warn("Ошибка при возврате автокомита в исходное состояние.", e);
        }

        try {
            if (lastTargetAutoCommit != null) {
                targetConnection.setAutoCommit(lastTargetAutoCommit);
            }
        } catch (SQLException sqlException) {
            // Ошибка может возникнуть если во время операции упало
            // соединение к БД
            LOG.warn("Ошибка при возврате автокомита в исходное состояние.", sqlException);
        }
    }

    /**
     * Получение привязки списка раннеров к именам таблиц в текущей БД
     * 
     * @return
     */
    public Map<String, Collection<RunnerModel>> getTableObservers(HikariCPSettingsModel sourcePool) {
        Map<String, Collection<RunnerModel>> tableObservers = 
                new TreeMap<String, Collection<RunnerModel>>(String.CASE_INSENSITIVE_ORDER);
        for (RunnerModel runner : sourcePool.getRunners()) {
            for (TableModel table : runner.getTables()) {
                String tableName = table.getName();
                Collection<RunnerModel> observers = tableObservers.get(tableName);
                if (observers==null) {
                    observers = new ArrayList<RunnerModel>();
                    tableObservers.put(tableName, observers);
                }
                observers.add(runner);
            }
        }
        return tableObservers;
    }

    /**
     * Получение списка раннеров, запускаемых из тасков
     * @return
     */
    protected Set<Runner> getRunnersFromTask() {
        if (tRunners == null) {
            TaskSettingsService taskSettingsService = Core.getTaskSettingsService();
            tRunners = new HashSet<>();
            for (TaskSettings task : taskSettingsService.getTasks().values()) {
                tRunners.add(task.getRunner());
            }
        }

        return tRunners;
    }

    /**
     * Переопределяемый метод для запуска раннеров
     * 
     * @param runners
     * @throws StrategyException
     * @throws SQLException
     */
    protected abstract void startRunners(Collection<RunnerModel> runners) throws StrategyException, SQLException;
}
