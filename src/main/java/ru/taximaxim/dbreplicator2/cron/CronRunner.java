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
package ru.taximaxim.dbreplicator2.cron;

import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobDataMap;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import ru.taximaxim.dbreplicator2.replica.StrategyException;
import ru.taximaxim.dbreplicator2.el.ErrorsLog;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;
import ru.taximaxim.dbreplicator2.utils.Core;
import ru.taximaxim.dbreplicator2.model.CronSettings;

/**
 * Класс потока циклического запуска выполнения обработчика записей
 *
 * @author volodin_aa
 *
 */
@DisallowConcurrentExecution
public class CronRunner implements Job {

    public static final Logger LOG = Logger.getLogger(CronRunner.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        JobDataMap jobDataMap = context.getJobDetail().getJobDataMap();
        CronSettings cronSettings = (CronSettings) jobDataMap.get("task");
        WorkerThread workerThread = new WorkerThread(cronSettings.getRunner());

        LOG.info(String.format("Запуск задачи [%d] %s",
                cronSettings.getTaskId(), cronSettings.getDescription()));

        try (ErrorsLog errorsLog = Core.getErrorsLog();){
            workerThread.processCommand();
        } catch (InstantiationException | IllegalAccessException e) {
            LOG.error(
                    String.format("Ошибка при создании объекта-стратегии раннера задачи [id_task = %d, %s]", 
                            cronSettings.getTaskId(),
                            cronSettings.getDescription()), 
                            e);
        } catch (ClassNotFoundException e) {
            LOG.error(
                    String.format("Ошибка инициализации при выполнении задачи [id_task = %d, %s]",
                            cronSettings.getTaskId(),
                            cronSettings.getDescription()), e);
        } catch (StrategyException e) {
            LOG.error(
                    String.format("Ошибка при выполнении стратегии из задачи [id_task = %d, %s]",
                            cronSettings.getTaskId(),
                            cronSettings.getDescription()), e);
        } catch (SQLException e) {
            LOG.error(
                    String.format("Ошибка БД при выполнении стратегии из задачи [id_task = %d, %s]",
                            cronSettings.getTaskId(),
                            cronSettings.getDescription()), e);
            SQLException nextEx = e.getNextException();
            while (nextEx!=null){
                LOG.error("Подробности:", nextEx);
                nextEx = nextEx.getNextException();
            }
        } catch (Exception e) {
            LOG.error(
                    String.format("Ошибка при выполнении задачи [id_task = %d, %s]",
                            cronSettings.getTaskId(),
                            cronSettings.getDescription()), e);
        }
    }
}
