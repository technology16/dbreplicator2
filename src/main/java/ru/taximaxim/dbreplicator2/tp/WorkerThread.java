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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.model.Runner;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.StopChainProcesing;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;
import ru.taximaxim.dbreplicator2.el.ErrorsLog;
import ru.taximaxim.dbreplicator2.utils.Core;
/**
 * Обработчик раннера
 * 
 * @author volodin_aa
 *
 */
public class WorkerThread implements Runnable {

    private static final Logger LOG = Logger.getLogger(WorkerThread.class);
    private Runner runner;

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
        LOG.debug(String.format("Запуск потока [%s] раннера [id_runner = %d, %s]...",
                Thread.currentThread().getName(), runner.getId(),
                runner.getDescription()));

        try (ErrorsLog errorsLog = Core.getErrorsLog();){
            try {
                processCommand(errorsLog);
            } catch (ClassNotFoundException e) {
                errorsLog.add(runner.getId(), null, null, 
                        String.format("Ошибка при инициализации данных раннера [id_runner = %d, %s]", 
                                runner.getId(), runner.getDescription()), e);
            } catch (StrategyException e) {
                errorsLog.add(runner.getId(), null, null, 
                        String.format("Ошибка при выполнении стратегии раннера [id_runner = %d, %s]", 
                                runner.getId(), runner.getDescription()), e);
            } catch (SQLException e) {
                errorsLog.add(runner.getId(), null, null, 
                        String.format("Ошибка БД при выполнении стратегии раннера [id_runner = %d, %s]", 
                                runner.getId(), runner.getDescription()), e);
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
     */
    public void processCommand(ErrorsLog errorsLog) throws ClassNotFoundException, SQLException, StrategyException {

        ConnectionFactory connectionsFactory = Core.getConnectionFactory();

        // Инициализируем два соединения. Используем try-with-resources для
        // каждого.
        try (Connection sourceConnection =
                connectionsFactory.getConnection(runner.getSource().getPoolId());
                Connection targetConnection =
                        connectionsFactory.getConnection(runner.getTarget().getPoolId())
                ) {
            List<StrategyModel> strategies = runner.getStrategyModels();

            try {
                for (StrategyModel strategyModel : strategies) {
                    if (strategyModel.isEnabled()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(String.format("runStrategy(%s, %s, %s);\n" +
                                    "sourceConnection = %s;\n" +
                                    "targetConnection = %s;", 
                                    runner.getSource().getPoolId(),
                                    runner.getTarget().getPoolId(),
                                    strategyModel.getId(),
                                    sourceConnection.hashCode(),
                                    targetConnection.hashCode()));
                        }
                        runStrategy(sourceConnection, targetConnection, strategyModel);
                    }
                }
            } catch (StopChainProcesing e) {
                LOG.warn(String.format("Запрошена принудительная остановка обработки цепочки стратегий раннера [id_runner = %d, %s].", 
                        runner.getId(), runner.getDescription()), e);
            }

        } catch (InstantiationException | IllegalAccessException e) {
            errorsLog.add(runner.getId(), null, null, 
                    String.format("Ошибка при создании объекта-стратегии раннера [id_runner = %d, %s]. [%s]", 
                            runner.getId(), runner.getDescription(), e));
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
     * @throws SQLException 
     */
    protected void runStrategy(Connection sourceConnection,
            Connection targetConnection, StrategyModel strategyModel)
                    throws ClassNotFoundException, InstantiationException,
                    IllegalAccessException, StrategyException, SQLException {

        Class<?> clazz = Class.forName(strategyModel.getClassName());
        Strategy strategy = (Strategy) clazz.newInstance();

        strategy.execute(sourceConnection, targetConnection, strategyModel);
    }

    /**
     * @return the runner
     */
    protected Runner getRunner() {
        return runner;
    }
}
