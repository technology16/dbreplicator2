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

import java.sql.SQLException;
import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.model.Runner;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.el.DefaultUncaughtExceptionHandler;
import ru.taximaxim.dbreplicator2.el.ErrorsLog;
import ru.taximaxim.dbreplicator2.el.FatalReplicationException;
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
            } catch (FatalReplicationException e) {
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
     * @param runner
     *            Настроенный runner.
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws StrategyException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public void processCommand() throws FatalReplicationException {
        ConnectionFactory connectionsFactory = Core.getConnectionFactory();
        for (StrategyModel strategyModel : runner.getStrategyModels()) {
            if (strategyModel.isEnabled()) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug(String.format("Запускаем стратегию [%s, %s]);", strategyModel.getClassName(),
                            strategyModel.getId()));
                }

                runStrategy(connectionsFactory, strategyModel);
            }
        }
    }

    /**
     * Выполняет запрошенную стратегию.
     *
     * @param sourceConnection
     *            Источник
     * @param targetConnection
     *            Целевая БД
     * @param strategyModel
     *            Данные для стратегии
     *
     * @throws FatalReplicationException 
     */
    protected void runStrategy(ConnectionFactory connectionsFactory, StrategyModel strategyModel)
            throws FatalReplicationException {
        try {
            Class<?> clazz = Class.forName(strategyModel.getClassName());
            Strategy strategy = (Strategy) clazz.newInstance();
            strategy.execute(connectionsFactory, strategyModel);
        } catch (ClassNotFoundException e) {
            throw new FatalReplicationException(String.format("Класс [%s] стратегии [id = %d] не найден ",
                    strategyModel.getClassName(), strategyModel.getId()), e);
        } catch (InstantiationException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка при создании объекта-стратегии [id = %d]", strategyModel.getId()), e);
        } catch (IllegalAccessException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка доступа к объекту-стратегии [id = %d]", strategyModel.getId()), e);
        } catch (SQLException e) {
            throw new FatalReplicationException(
                    String.format("Ошибка выполнения стратегии [id = %d]", strategyModel.getId()), e);
        }
    }

    /**
     * @return the runner
     */
    protected Runner getRunner() {
        return runner;
    }
}
