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
import java.util.Date;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.replica.StrategyException;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;
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
     * Метод для корректной остановки потока задачи. Поток дождется окончания текущего цикла и завершит работу
     */
    public void stop(){
        enabled = false;
    }

    protected void wait(long startTime, long interval, String message) throws InterruptedException {
        long sleepTime = startTime + interval - new Date().getTime();
        if (sleepTime > 0) {
            LOG.info(String.format(message, sleepTime));
            Thread.sleep(sleepTime);
        }
    }
    
    @Override
    public void run() {
        LOG.info(String.format("Запуск задачи [%d] %s",
                taskSettings.getTaskId(), taskSettings.getDescription()));
        while (enabled) {
            long startTime = new Date().getTime();
            boolean isSuccess = false;
            
            try {
                workerThread.processCommand();
                isSuccess = true;
            } catch (ClassNotFoundException e) {
                LOG.error(
                        String.format("Ошибка инициализации при выполнении задачи [%d] %s",
                                taskSettings.getTaskId(),
                                taskSettings.getDescription()), e);
            } catch (StrategyException e) {
                LOG.error(
                        String.format("Ошибка при выполнении стратегии из задачи [%d] %s",
                        taskSettings.getTaskId(),
                        taskSettings.getDescription()), e);
            } catch (SQLException e) {
                LOG.error(
                        String.format("Ошибка БД при выполнении стратегии из задачи [%d] %s",
                        taskSettings.getTaskId(),
                        taskSettings.getDescription()), e);
                SQLException nextEx = e.getNextException();
                while (nextEx!=null){
                    LOG.error("Подробности:", nextEx);
                    nextEx = nextEx.getNextException();
                }
            } catch (Exception e) {
                LOG.error(
                        String.format("Ошибка при выполнении задачи [%d] %s",
                                taskSettings.getTaskId(),
                                taskSettings.getDescription()), e);
            }
            
            try {
                if (isSuccess) {
                    // Ожидаем окончания периода синхронизации
                    wait(startTime, taskSettings.getSuccessInterval(), 
                            String.format("Ожидаем %s милисекунд после завершения задачи [%d] %s",
                                    "%d", taskSettings.getTaskId(),
                                    taskSettings.getDescription()));
                } else {
                    wait(startTime, taskSettings.getFailInterval(), 
                        String.format(
                                "Ожидаем %s миллисекунд до рестарта задачи [%d] %s",
                                "%d", taskSettings.getTaskId(), 
                                taskSettings.getDescription()));
                }
            } catch (InterruptedException ex) {
                LOG.warn(ex);
            }
        }
    }
}
