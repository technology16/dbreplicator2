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

package ru.taximaxim.dbreplicator2.tp;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.el.DefaultUncaughtExceptionHandler;
import ru.taximaxim.dbreplicator2.el.ErrorsLog;
import ru.taximaxim.dbreplicator2.model.HikariCPSettingsModel;
import ru.taximaxim.dbreplicator2.model.Runner;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Обработчик раннера
 *
 * @author volodin_aa
 *
 */
public class WorkerThread implements Runnable {

    private static final Logger LOG = Logger.getLogger(WorkerThread.class);
    private final Runner runner;

    /**
     * Конструктор на основе настроек раннера
     *
     * @param runner
     */
    public WorkerThread(Runner runner) {
        this.runner = runner;
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(
                new DefaultUncaughtExceptionHandler(runner.getId()));

        LOG.debug(String.format("Запуск потока [%s] раннера [id_runner = %d, %s]...",
                Thread.currentThread().getName(), runner.getId(),
                runner.getDescription()));

        try (ErrorsLog errorsLog = Core.getErrorsLog();) {
            try {
                processCommand();
                errorsLog.setStatus(runner.getId(), null, null, 1);
            } catch (InstantiationException | IllegalAccessException
                    | NoSuchMethodException | InvocationTargetException e) {
                errorsLog.add(runner.getId(), null, null,
                        String.format(
                                "Ошибка при создании объекта-стратегии раннера [id_runner = %d, %s]. [%s]",
                                runner.getId(), runner.getDescription(), e));
            } catch (ClassNotFoundException e) {
                errorsLog.add(runner.getId(), null, null,
                        String.format(
                                "Ошибка при инициализации данных раннера [id_runner = %d, %s]",
                                runner.getId(), runner.getDescription()),
                        e);
            } catch (SQLException e) {
                errorsLog.add(runner.getId(), null, null,
                        String.format(
                                "Ошибка БД при выполнении стратегии раннера [id_runner = %d, %s]",
                                runner.getId(), runner.getDescription()),
                        e);
            }
        }

        LOG.debug(String.format("Завершение потока [%s] раннера [id_runner = %d, %s]",
                Thread.currentThread().getName(), runner.getId(),
                runner.getDescription()));
    }

    /**
     * Запуск рабочего потока. Вся работа по выполнению репликации должна
     * выполняться здесь.
     *
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     */
    public void processCommand()
            throws ClassNotFoundException, SQLException, InstantiationException,
            IllegalAccessException, InvocationTargetException, NoSuchMethodException {

        HikariCPSettingsModel sourceSettings = runner.getSource();
        if (sourceSettings != null && !sourceSettings.isEnabled()) {
            LOG.warn(String.format(
                    "Раннер [id_runner = %d, %s] : база-источник [%s] отключена настройками",
                    runner.getId(), runner.getDescription(), sourceSettings.getPoolId()));
            return;
        }

        HikariCPSettingsModel targetSettings = runner.getTarget();
        if (targetSettings != null && !targetSettings.isEnabled()) {
            LOG.warn(String.format(
                    "Раннер [id_runner = %d, %s] : база-приемник [%s] отключена настройками",
                    runner.getId(), runner.getDescription(), targetSettings.getPoolId()));
            return;
        }

        ConnectionFactory connectionsFactory = Core.getConnectionFactory();
        for (StrategyModel strategyModel : runner.getStrategyModels()) {
            if (strategyModel.isEnabled()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Запускаем стратегию [%s, %s]);",
                            strategyModel.getClassName(), strategyModel.getId()));
                }

                runStrategy(connectionsFactory, strategyModel);
            }
        }
    }

    /**
     * Выполняет запрошенную стратегию.
     *
     * @param connectionsFactory
     *            Фабрика подключений
     * @param strategyModel
     *            Данные для стратегии
     *
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws SQLException
     * @throws NoSuchMethodException
     * @throws InvocationTargetException
     * @throws IllegalArgumentException
     */
    protected void runStrategy(ConnectionFactory connectionsFactory, StrategyModel strategyModel)
            throws SQLException, ClassNotFoundException, InstantiationException,
            IllegalAccessException, InvocationTargetException, NoSuchMethodException {

        Class<?> clazz = Class.forName(strategyModel.getClassName());
        Strategy strategy = (Strategy) clazz.getConstructor().newInstance();

        strategy.execute(connectionsFactory, strategyModel);
    }

    /**
     * @return the runner
     */
    protected Runner getRunner() {
        return runner;
    }
}
