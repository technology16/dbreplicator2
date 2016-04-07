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
package ru.taximaxim.dbreplicator2.tasks;

import java.sql.SQLException;
import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.tp.WorkerThread;
import ru.taximaxim.dbreplicator2.utils.Watch;
import ru.taximaxim.dbreplicator2.model.TaskSettings;

/**
 * Класс потока циклического запуска выполнения обработчика записей
 *
 * @author volodin_aa
 *
 */
public class TaskRunner implements Runnable {

    public static final Logger LOG = Logger.getLogger(TaskRunner.class);

    /**
     * Текущие настройки задачи
     */
    private TaskSettings taskSettings;

    /**
     * Инициализированный рабочий поток
     */
    private WorkerThread workerThread;

    /**
     * Флаг активности задачи
     */
    private boolean enabled;

    /**
     * @param taskSettings
     */
    public TaskRunner(TaskSettings taskSettings) {
        this.taskSettings = taskSettings;
        this.workerThread = new WorkerThread(taskSettings.getRunner());

        enabled = true;
    }

    /**
     * Метод для корректной остановки потока задачи. Поток дождется окончания
     * текущего цикла и завершит работу
     */
    public void stop() {
        enabled = false;
    }

    @Override
    public void run() {
        LOG.info(String.format("Запуск задачи [%d] %s", taskSettings.getTaskId(),
                taskSettings.getDescription()));
        final Watch successWatch = new Watch(taskSettings.getSuccessInterval());
        final Watch failWatch = new Watch(taskSettings.getFailInterval());
        try {
            while (enabled) {
                try {
                    workerThread.processCommand();

                    if (taskSettings.getSuccessInterval() == null) {
                        stop();
                    } else {
                        // Ожидаем окончания периода синхронизации
                        long sleepTime = successWatch.remaining();
                        if (sleepTime > 0) {
                            LOG.info(String.format(
                                    "Ожидаем %d милисекунд после завершения задачи [id_task = %d, %s]",
                                    sleepTime, taskSettings.getTaskId(),
                                    taskSettings.getDescription()));
                        }
                        successWatch.sleep();
                        failWatch.start();
                    }
                    
                    continue;
                } catch (InstantiationException | IllegalAccessException e) {
                    LOG.error(String.format(
                            "Ошибка при создании объекта-стратегии раннера задачи [id_task = %d, %s]",
                            taskSettings.getTaskId(), taskSettings.getDescription()), e);
                } catch (ClassNotFoundException e) {
                    LOG.error(String.format(
                            "Ошибка инициализации при выполнении задачи [id_task = %d, %s]",
                            taskSettings.getTaskId(), taskSettings.getDescription()), e);
                } catch (SQLException e) {
                    LOG.error(String.format(
                            "Ошибка БД при выполнении стратегии из задачи [id_task = %d, %s]",
                            taskSettings.getTaskId(), taskSettings.getDescription()), e);
                    SQLException nextEx = e.getNextException();
                    while (nextEx != null) {
                        LOG.error("Подробности:", nextEx);
                        nextEx = nextEx.getNextException();
                    }
                } catch (InterruptedException ex) {
                    LOG.warn(ex);
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    LOG.error(String.format(
                            "Ошибка при выполнении задачи [id_task = %d, %s]",
                            taskSettings.getTaskId(), taskSettings.getDescription()), e);
                }

                if (taskSettings.getFailInterval() == null) {
                    stop();
                } else {
                    long sleepTime = failWatch.remaining();
                    if (sleepTime > 0) {
                        LOG.info(String.format(
                                "Ожидаем %d миллисекунд до рестарта задачи [id_task = %d, %s]",
                                sleepTime, taskSettings.getTaskId(),
                                taskSettings.getDescription()));
                    }
                    failWatch.sleep();
                    successWatch.start();
                }
            }
        } catch (InterruptedException ex) {
            LOG.warn(ex);
            Thread.currentThread().interrupt();
        }
    }
}