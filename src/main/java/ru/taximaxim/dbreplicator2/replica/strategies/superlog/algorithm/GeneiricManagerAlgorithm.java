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
import ru.taximaxim.dbreplicator2.model.HikariCPSettingsModel;
import ru.taximaxim.dbreplicator2.model.Runner;
import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.model.TableModel;
import ru.taximaxim.dbreplicator2.model.TaskSettings;
import ru.taximaxim.dbreplicator2.model.TaskSettingsService;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.workpool.WorkPoolService;
import ru.taximaxim.dbreplicator2.replica.strategies.superlog.data.SuperlogDataService;
import ru.taximaxim.dbreplicator2.utils.Core;
import ru.taximaxim.dbreplicator2.utils.Watch;

/**
 * Класс стратегии менеджера записей суперлог таблицы
 * 
 * @author volodin_aa
 * 
 */
public abstract class GeneiricManagerAlgorithm {

    private static final String SUPER_LOG_PERIOD_PARAM = "superLogPeriod";
    private static final String START_ALL_RUNNERS_PERIOD_PARAM = "startAllRunnersPeriod";

    private static final long SUPER_LOG_PERIOD = 400;
    private static final long START_ALL_RUNNERS_PERIOD = 10000;

    private static final Logger LOG = Logger.getLogger(GeneiricManagerAlgorithm.class);

    protected final SuperlogDataService superlogDataService;

    private Set<Runner> tRunners;

    private final StrategyModel data;

    /**
     * Конструктор по умолчанию
     */
    public GeneiricManagerAlgorithm(SuperlogDataService superlogDataService,
            StrategyModel data) {
        this.superlogDataService = superlogDataService;
        this.data = data;
    }

    /**
     * @return
     */
    protected long getSuperLogPeriod() {
        final String superLogPeriod = data.getParam(SUPER_LOG_PERIOD_PARAM);
        if (superLogPeriod == null) {
            return SUPER_LOG_PERIOD;
        }
        return Long.parseLong(superLogPeriod);
    }

    /**
     * @return
     */
    protected long getStartAllRunnersPeriod() {
        final String superLogWatchParam = data.getParam(START_ALL_RUNNERS_PERIOD_PARAM);
        if (superLogWatchParam == null) {
            return START_ALL_RUNNERS_PERIOD;
        }
        return Long.parseLong(superLogWatchParam);
    }

    /**
     * Вставка данных в ворк пул. Возвращает true если у таблицы были
     * обработчики
     * 
     * @param superLogResult
     * @param deleteSuperLog
     * @param insertRunnerData
     * @param runners
     * @param tableName
     * @param observers
     * 
     * @throws SQLException
     */
    protected int insertRunnersData(ResultSet superLogResult,
            PreparedStatement insertRunnerData, PreparedStatement deleteSuperLog,
            Map<String, Collection<RunnerModel>> tableObservers, Set<RunnerModel> runners)
            throws SQLException {
        final long idSuperLog = superLogResult.getLong(WorkPoolService.ID_SUPERLOG);
        final String tableName = superLogResult.getString(WorkPoolService.ID_TABLE);
        final Collection<RunnerModel> observers = tableObservers.get(tableName);
        if (observers != null) {
            // Заполняем "константы"
            insertRunnerData.setLong(2, idSuperLog);
            insertRunnerData.setInt(3, superLogResult.getInt(WorkPoolService.ID_FOREIGN));
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
            return 1;
        }
        return 0;
    }

    /**
     * Извлекает данные из супер лога и разносит по раннерам в ворк пуле.
     * Работает пока очередная выборка не вернет 0 строк.
     * 
     * @param superLog
     * @param tableObservers
     * @param selectService
     * @param deleteService
     * @return суммарное количество обработанных строк
     * @throws SQLException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    protected int writeToWorkPool(Future<ResultSet> initSuperLog,
            Map<String, Collection<RunnerModel>> tableObservers,
            ExecutorService selectService, ExecutorService deleteService,
            Watch startAllRunnersWatch, long startAllRunnersPeriod)
            throws SQLException, InterruptedException, ExecutionException {
        int rowsCount = 0;
        long idSuperLog = 0;
        boolean hasRows;
        final Set<RunnerModel> runners = new HashSet<RunnerModel>();
        Future<int[]> deleteSuperLogResult = null;
        Future<ResultSet> superLog = initSuperLog;

        do {
            hasRows = false;
            // Извлекаем очередную порцию данных
            try (final ResultSet superLogResult = superLog.get()) {
                waitDeleteSuperlog(deleteSuperLogResult);

                while (superLogResult.next()) {
                    // Копируем записи
                    idSuperLog = superLogResult.getLong(WorkPoolService.ID_SUPERLOG);
                    rowsCount += insertRunnersData(superLogResult,
                            getInsertWorkpoolStatement(), getDeleteSuperlogStatement(),
                            tableObservers, runners);
                    hasRows = true;
                }
            }

            // Если в выборке были данные, то в параллельном потоке
            // запускаем чтение новых данных
            if (hasRows) {
                getSelectSuperlogStatement().setLong(1, idSuperLog);
                superLog = selectService
                        .submit(new QueryCall(getSelectSuperlogStatement()));

                // Сбрасываем данные в базу
                deleteSuperLogResult = executeBatches(deleteService,
                        getDeleteSuperlogStatement(), getInsertWorkpoolStatement());

                // запускаем обработчики реплик
                if (startAllRunnersWatch.timeOut(startAllRunnersPeriod)) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug(String.format(
                                "Раннер [id_runner = %d, %s], стратегия [id = %d] запустила все раннеры",
                                data.getRunner().getId(),
                                data.getRunner().getDescription(), data.getId()));
                    }
                    startAllRunners(tableObservers);
                    startAllRunnersWatch.start();
                } else {
                    startRunners(runners);
                }
                runners.clear();

                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format(
                            "Раннер [id_runner = %d, %s], стратегия [id = %d] обработано %d строк",
                            data.getRunner().getId(), data.getRunner().getDescription(),
                            data.getId(), rowsCount));
                }
            }
        } while (hasRows);

        return rowsCount;
    }

    /**
     * Запуск алгоритма обработки супер лога
     * 
     * @throws SQLException
     */
    public void execute() throws SQLException {
        // Выборку данных будем выполнять в отдельном потоке
        final ExecutorService selectService = Executors.newSingleThreadExecutor();
        // Переносим данные
        try {
            final PreparedStatement initSelectSuperLog = getInitSelectSuperlogStatement();
            Future<ResultSet> superLog = selectService
                    .submit(new QueryCall(initSelectSuperLog));
            // Начинаем отсчет времени
            final Watch superLogWatch = new Watch();
            final Watch startAllRunnersWatch = new Watch();

            final long superLogPeriod = getSuperLogPeriod();
            final long startAllRunnersPeriod = getStartAllRunnersPeriod();

            // Строим список обработчиков реплик
            final Map<String, Collection<RunnerModel>> tableObservers = getTableObservers(
                    data.getRunner().getSource());

            // При старте принудительно запускаем все обработчики
            startAllRunners(tableObservers);

            // Удалени данных будем выполнять в фоновой очереди заданий
            final ExecutorService deleteService = Executors.newSingleThreadExecutor();

            int totalRowsCount = 0;
            try {
                do {
                    totalRowsCount = writeToWorkPool(superLog, tableObservers,
                            selectService, deleteService, startAllRunnersWatch,
                            startAllRunnersPeriod);

                    // Если на предыдущей итерации была обработана хотя бы одна
                    // строка, то делаем мягкую задержку
                    if (totalRowsCount > 0) {
                        superLogWatch.sleep(superLogPeriod);
                        // и извлекаем данные с начала
                        superLog = selectService
                                .submit(new QueryCall(initSelectSuperLog));
                    }
                } while (totalRowsCount > 0);
            } finally {
                deleteService.shutdown();
            }
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
     * @param deleteSuperLogResult
     * @throws InterruptedException
     * @throws ExecutionException
     * @throws SQLException
     */
    protected void waitDeleteSuperlog(Future<int[]> deleteSuperLogResult)
            throws InterruptedException, ExecutionException {
        if (deleteSuperLogResult != null) {
            deleteSuperLogResult.get();
        }
    }

    /**
     * @param tableObservers
     * @param runners
     * @throws SQLException
     */
    protected void startAllRunners(Map<String, Collection<RunnerModel>> tableObservers)
            throws SQLException {
        final Set<RunnerModel> runners = new HashSet<RunnerModel>();
        // запускаем все обработчики реплик
        for (Collection<RunnerModel> observers : tableObservers.values()) {
            runners.addAll(observers);
        }
        startRunners(runners);
    }

    /**
     * @return
     * @throws SQLException
     */
    protected PreparedStatement getInitSelectSuperlogStatement() throws SQLException {
        return superlogDataService.getInitSelectSuperlogStatement();
    }

    /**
     * @return
     * @throws SQLException
     */
    protected PreparedStatement getSelectSuperlogStatement() throws SQLException {
        return superlogDataService.getSelectSuperlogStatement();
    }

    /**
     * @return
     * @throws SQLException
     */
    protected PreparedStatement getInsertWorkpoolStatement() throws SQLException {
        return superlogDataService.getInsertWorkpoolStatement();
    }

    /**
     * @return
     * @throws SQLException
     */
    protected PreparedStatement getDeleteSuperlogStatement() throws SQLException {
        return superlogDataService.getDeleteSuperlogStatement();
    }

    /**
     * Сбрасываем данные в базу Метод удаляет данные из суперлога в отдельном
     * потоке
     * 
     * @param service
     * @param deleteSuperLog
     * @param insertRunnerData
     * @return
     * @throws SQLException
     */
    protected Future<int[]> executeBatches(ExecutorService service,
            PreparedStatement deleteSuperLog, PreparedStatement insertRunnerData)
            throws SQLException {
        Future<int[]> deleteSuperLogResult = null;
        try {
            insertRunnerData.executeBatch();
            superlogDataService.getTargetConnection().commit();
            // Удаляем данные в очереди с выборкой новой порции
            deleteSuperLogResult = service.submit(new BatchCall(deleteSuperLog));
        } catch (SQLException e) {
            LOG.warn("Ошибка вставки записей в rep2_workpool_data:", e);
            superlogDataService.getTargetConnection().rollback();
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
        final Map<String, Collection<RunnerModel>> tableObservers = new TreeMap<String, Collection<RunnerModel>>(
                String.CASE_INSENSITIVE_ORDER);
        for (RunnerModel runner : sourcePool.getRunners()) {
            for (TableModel table : runner.getTables()) {
                final String tableName = table.getName();
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
            final TaskSettingsService taskSettingsService = Core.getTaskSettingsService();
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
            throws SQLException;
}
