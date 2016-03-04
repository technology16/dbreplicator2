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

    private CronSettingsService cronSettingsService;

    /**
     * Конструктор на основе сервиса настроек задач по расписанию
     * 
     * @param cronSettingsService
     */
    public CronPool(CronSettingsService cronSettingsService) {
        this.cronSettingsService = cronSettingsService;
    }

    /**
     * Запускаем потоки задач
     */
    public void start() {
        try {
            Scheduler scheduler = new StdSchedulerFactory().getScheduler();
            Map<Integer, CronSettings> taskSettings = cronSettingsService.getTasks();
            boolean hasTasks = false;
            for (CronSettings task : taskSettings.values()) {
                if (task.getEnabled()) {
                    JobDetail jobDetail = JobBuilder.newJob(CronRunner.class).build();
                    jobDetail.getJobDataMap().put("task", task);
                    Trigger trigger = TriggerBuilder.newTrigger()
                            .withSchedule(CronScheduleBuilder
                                    .cronSchedule(task.getCronString())).build();
                    try {
                        scheduler.scheduleJob(jobDetail, trigger);
                        hasTasks = true;
                    } catch (SchedulerException e) {
                        LOG.error(String.format("Ошибка при старте задачи task[id = %s]!", task.getTaskId()), e);
                    }
                }
            }
            // Запускаем если только у нас есть задания
            if (hasTasks) {
                scheduler.start();
            } else {
                scheduler.shutdown();
            }
        } catch (SchedulerException e) {
            LOG.error("Ошибка при старте планировщика задач!", e);
        }
    }
}
