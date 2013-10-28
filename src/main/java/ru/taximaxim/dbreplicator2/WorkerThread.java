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

package ru.taximaxim.dbreplicator2;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.Runner;
import ru.taximaxim.dbreplicator2.replica.StopChainProcesing;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;

public class WorkerThread implements Runnable {

    private static final Logger LOG = Logger.getLogger(WorkerThread.class);

    private Runner runner;

    public WorkerThread(Runner runner) {
        this.runner = runner;
    }

    public void run() {

        LOG.debug(String.format("Запуск потока: %s [%s] [%s]",
                runner.getDescription(), runner.getId(),
                Thread.currentThread().getName()));

        processCommand(runner);

        LOG.debug(String.format("Завершение потока: %s [%s] [%s]",
                runner.getDescription(), runner.getId(),
                Thread.currentThread().getName()));
    }

    /**
     * Запуск рабочего потока. Вся работа по выполнению репликации должна
     * выполняться здесь.
     *
     * @param runner
     *            Настроенный runner.
     */
    protected void processCommand(Runner runner) {

        ConnectionFactory connectionsFactory = Application.getConnectionFactory();

        // Инициализируем два соединения. Используем try-with-resources для
        // каждого.
        try (Connection sourceConnection =
                connectionsFactory.getConnection(runner.getSource())) {
            try (Connection targetConnection =
                    connectionsFactory.getConnection(runner.getTarget())) {

                List<StrategyModel> strategies = runner.getStrategyModels();

                try {
                    for (StrategyModel strategyModel : strategies) {
                        if (strategyModel.isEnabled()) {
                            runStrategy(sourceConnection, targetConnection, strategyModel);
                        }
                    }
                } catch (StopChainProcesing e) {
                    LOG.warn("Запрошена принудительная остановка обработка цепочки.", e);
                }

            } catch (InstantiationException | IllegalAccessException e) {
                LOG.error("Ошибка при создании объекта-стратегии", e);
            }
        } catch (ClassNotFoundException | SQLException e) {
            LOG.error("Ошибка при инициализации соединений с базами данных " +
                    "потока-воркера", e);
        } catch (StrategyException e) {
            LOG.error("Ошибка при выполнении стратегии", e);
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
     * @throws ClassNotFoundException
     * @throws InstantiationException
     * @throws IllegalAccessException
     * @throws StrategyException
     */
    protected void runStrategy(Connection sourceConnection,
            Connection targetConnection, StrategyModel strategyModel)
            throws ClassNotFoundException, InstantiationException,
            IllegalAccessException, StrategyException {

        Class<?> clazz = Class.forName(strategyModel.getClassName());
        Strategy strategy = (Strategy) clazz.newInstance();

        strategy.execute(sourceConnection, targetConnection, strategyModel);
    }
}
