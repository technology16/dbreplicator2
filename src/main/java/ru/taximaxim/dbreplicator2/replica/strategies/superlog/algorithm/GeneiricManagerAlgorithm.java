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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.jdbc.BatchCall;
import ru.taximaxim.dbreplicator2.jdbc.QueryCall;
import ru.taximaxim.dbreplicator2.jdbc.ResultSetCloseCall;
import ru.taximaxim.dbreplicator2.model.HikariCPSettingsModel;
import ru.taximaxim.dbreplicator2.model.Runner;
import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.model.TableModel;
import ru.taximaxim.dbreplicator2.model.TaskSettings;
import ru.taximaxim.dbreplicator2.model.TaskSettingsService;
import ru.taximaxim.dbreplicator2.replica.StrategyException;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.workpool.WorkPoolService;
import ru.taximaxim.dbreplicator2.replica.strategies.superlog.data.SuperlogDataService;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Класс стратегии менеджера записей суперлог таблицы
 * 
 * @author volodin_aa
 * 
 */
public abstract class GeneiricManagerAlgorithm {

    private static final Logger LOG = Logger.getLogger(GeneiricManagerAlgorithm.class);

    protected SuperlogDataService superlogDataService;

    private Set<Runner> tRunners;

    /**
     * Конструктор по умолчанию
     */
    public GeneiricManagerAlgorithm(SuperlogDataService superlogDataService) {
        this.superlogDataService = superlogDataService;
    }

    /**
     * Вставка данных в ворк пул
     * 
     * @param superLogResult
     * @param deleteSuperLog
     * @param insertRunnerData
     * @param runners
     * @param tableName
     * @param observers
     * @throws SQLException
     */
    protected long insertRunnersData(ResultSet superLogResult,
            PreparedStatement insertRunnerData, PreparedStatement deleteSuperLog,
            Map<String, Collection<RunnerModel>> tableObservers, Set<RunnerModel> runners)
            throws SQLException {
        long idSuperLog = superLogResult.getLong(WorkPoolService.ID_SUPERLOG);
        String tableName = superLogResult.getString(WorkPoolService.ID_TABLE);
        Collection<RunnerModel> observers = tableObservers.get(tableName);
        if (observers != null) {
            // Заполняем "константы"
            insertRunnerData.setLong(2, idSuperLog);
            insertRunnerData.setInt(3,
                    superLogResult.getInt(WorkPoolService.ID_FOREIGN));
            insertRunnerData.setString(4, tableName);
            insertRunnerData.setString(5,
                    superLogResult.getString(WorkPoolService.C_OPERATION));
            insertRunnerData.setTimestamp(6,
                    superLogResult.getTimestamp(WorkPoolService.C_DATE));
            insertRunnerData.setString(7,
                    superLogResult.getString(WorkPoolService.ID_TRANSACTION));

            // Раскладываем данные по раннерам
            for (RunnerModel runner : observers) {
                // Игнорим данные у которых владелец совпадает с приемником
                if (!superLogResult.getString(WorkPoolService.ID_POOL)
                        .equals(runner.getTarget().getPoolId())) {
                    insertRunnerData.setInt(1, runner.getId());
                    insertRunnerData.addBatch();
                    runners.add(runner);
                }
            }
            // Удаляем исходную запись
            deleteSuperLog.setLong(1, idSuperLog);
            deleteSuperLog.addBatch();
        }
        return idSuperLog;
    }

    public void execute(StrategyModel data) throws StrategyException, SQLException {
        // Работаем в режиме автокоммита
        superlogDataService.getSelectConnection().setAutoCommit(true);
        superlogDataService.getSelectConnection()
                .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        superlogDataService.getDeleteConnection().setAutoCommit(true);
        superlogDataService.getDeleteConnection()
                .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        superlogDataService.getTargetConnection().setAutoCommit(true);
        superlogDataService.getTargetConnection()
                .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        ResultSet superLogResult;
        // Выборку данных будем выполнять в отдельном потоке
        ExecutorService selectService = Executors.newSingleThreadExecutor();
        // Переносим данные
        try (PreparedStatement initSelectSuperLog = superlogDataService
                .getInitSelectSuperlogStatement();
                PreparedStatement selectSuperLog = superlogDataService
                        .getSelectSuperlogStatement();) {
            Future<ResultSet> superLog = selectService
                    .submit(new QueryCall(initSelectSuperLog));

            // Строим список обработчиков реплик
            Map<String, Collection<RunnerModel>> tableObservers = getTableObservers(
                    data.getRunner().getSource());

            // Выборку данных будем выполнять в отдельном потоке
            ExecutorService deleteService = Executors.newSingleThreadExecutor();
            // Получаем соединение для удаления записей
            try (PreparedStatement deleteSuperLog = superlogDataService
                    .getDeleteSuperlogStatement();
                    PreparedStatement insertRunnerData = superlogDataService
                            .getInsertWorkpoolStatement();) {
                Set<RunnerModel> runners = new HashSet<RunnerModel>();

                int rowsCount = 0;
                long idSuperLog = 0;
                Future<int[]> deleteSuperLogResult;
                do {
                    // Дожидаемся выборки
                    rowsCount = 0;
                    superLogResult = superLog.get();
                    while (superLogResult.next()) {
                        // Копируем записи
                        idSuperLog = insertRunnersData(superLogResult, insertRunnerData,
                                deleteSuperLog, tableObservers, runners);
                        rowsCount++;
                    }

                    // Если в выборке были данные, то в параллельном потоке
                    // запускаем чтение новых данных
                    if (rowsCount > 0) {
                        selectSuperLog.setLong(1, idSuperLog);
                        superLog = selectService.submit(new QueryCall(selectSuperLog));
                    }

                    // Сбрасываем данные в базу
                    deleteSuperLogResult = executeBatches(deleteService,
                            deleteSuperLog, insertRunnerData);

                    // Закрываем ресурсы
                    selectService.submit(new ResultSetCloseCall(superLogResult));

                    // запускаем обработчики реплик
                    startRunners(runners);
                    runners.clear();
                    LOG.info(String.format("Обработано %s строк...", rowsCount));

                    // Дожидаемся удаления
                    if (deleteSuperLogResult != null) {
                        deleteSuperLogResult.get();
                    }
                } while (rowsCount > 0);
            } finally {
                deleteService.shutdown();
            }

            // запускаем все обработчики реплик
            Set<RunnerModel> runners = new HashSet<RunnerModel>();
            for (Collection<RunnerModel> observers : tableObservers.values()) {
                runners.addAll(observers);
            }
            startRunners(runners);
        } catch (InterruptedException e) {
            LOG.warn("Прервано получение данных из rep2_superlog!", e);
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOG.warn("Ошибка получения данных из rep2_superlog!", e);
        } finally {
            selectService.shutdown();
        }
    }

    /**
     * Сбрасываем данные в базу Метод удаляет данные из суперлога в отдельном
     * потоке
     * 
     * @param service
     * @param deleteSuperLog
     * @param insertRunnerData
     * @return
     */
    protected Future<int[]> executeBatches(ExecutorService service,
            PreparedStatement deleteSuperLog, PreparedStatement insertRunnerData) {
        Future<int[]> deleteSuperLogResult = null;
        try {
            insertRunnerData.executeBatch();
            // Удаляем данные в очереди с выборкой новой порции
            deleteSuperLogResult = service.submit(new BatchCall(deleteSuperLog));
        } catch (SQLException e) {
            LOG.warn(
                    String.format("Ошибка вставки записей в rep2_workpool_data:%n%s", e));
        }
        return deleteSuperLogResult;
    }

    /**
     * Получение привязки списка раннеров к именам таблиц в текущей БД
     * 
     * @return
     */
    public Map<String, Collection<RunnerModel>> getTableObservers(
            HikariCPSettingsModel sourcePool) {
        Map<String, Collection<RunnerModel>> tableObservers = new TreeMap<String, Collection<RunnerModel>>(
                String.CASE_INSENSITIVE_ORDER);
        for (RunnerModel runner : sourcePool.getRunners()) {
            for (TableModel table : runner.getTables()) {
                String tableName = table.getName();
                Collection<RunnerModel> observers = tableObservers.get(tableName);
                if (observers == null) {
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
     * 
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
    protected abstract void startRunners(Collection<RunnerModel> runners)
            throws StrategyException, SQLException;
}
