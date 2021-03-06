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

import java.text.ParseException;
import java.util.Map;

import org.apache.log4j.Logger;
import org.quartz.CronScheduleBuilder;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

import ru.taximaxim.dbreplicator2.model.CronSettings;
import ru.taximaxim.dbreplicator2.model.CronSettingsService;

/**
 * Пул задач по расписанию
 * 
 * @author volodin_aa
 * 
 */
public class CronPool {

    private static final Logger LOG = Logger.getLogger(CronPool.class);

    private final CronSettingsService cronSettingsService;

    private Scheduler scheduler;

    /**
     * Конструктор на основе сервиса настроек задач по расписанию
     * 
     * @param cronSettingsService
     * @throws SchedulerException
     */
    public CronPool(CronSettingsService cronSettingsService) {
        this.cronSettingsService = cronSettingsService;
    }

    /**
     * Запускаем потоки задач
     */
    public void start() {
        try {
            stop();
            
            scheduler = new StdSchedulerFactory().getScheduler();
            final Map<Integer, CronSettings> taskSettings = cronSettingsService
                    .getTasks();
            boolean hasTasks = false;
            for (CronSettings task : taskSettings.values()) {
                if (addTask(scheduler, task)) {
                    hasTasks = true;
                }
            }
            // Запускаем если только у нас есть задания
            if (hasTasks) {
                scheduler.start();
            } else {
                scheduler.shutdown();
            }
        } catch (SchedulerException e) {
            // Игнорим ошибку планировщика
            LOG.error("Ошибка при старте планировщика задач!", e);
        } catch (Throwable e) {
            // Пытаемся отправить сообщение об ошибке
            LOG.error("Непредвиденная ошибка при старте планировщика задач!", e);
            throw e;
        }
    }

    /**
     * Добавляем задание в планировщик
     * 
     * @param scheduler
     * @param hasTasks
     * @param task
     * @return
     * @throws RuntimeException
     */
    protected boolean addTask(Scheduler scheduler, CronSettings task) {
        if (task.getEnabled()) {
            try {
                Trigger trigger = TriggerBuilder.newTrigger()
                        .withSchedule(
                                CronScheduleBuilder.cronSchedule(task.getCronString()))
                        .build();
                JobDetail jobDetail = JobBuilder.newJob(CronRunner.class).build();
                jobDetail.getJobDataMap().put("task", task);
                scheduler.scheduleJob(jobDetail, trigger);

                return true;
            } catch (SchedulerException e) {
                // Игнорим ошибки кварца
                LOG.error(String.format("Ошибка при старте задачи cron [id = %s, %s]!",
                        task.getTaskId(), task.getDescription()), e);
            } catch (RuntimeException e) {
                // Игнорим ошибки парсера
                if (e.getCause() instanceof ParseException) {
                    LOG.error(String.format("Ошибка настройки задачи cron [id = %s, %s]!",
                            task.getTaskId(), task.getDescription()), e);
                } else {
                    throw e;
                }
            }
        }

        return false;
    }

    /**
     * Останавливаем потоки задач
     */
    public void stop() {
        if (scheduler == null) {
            return;
        }

        try {
            scheduler.shutdown();
        } catch (SchedulerException e) {
            // Игнорим ошибку планировщика
            LOG.error("Ошибка при остановки планировщика задач!", e);
        }
        scheduler = null;
    }

}
