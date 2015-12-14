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

import ru.taximaxim.dbreplicator2.model.TaskSettings;
import ru.taximaxim.dbreplicator2.model.TaskSettingsService;

/**
 * Пул задач по расписанию
 * 
 * @author volodin_aa
 * 
 */
public class TasksPool {

    private static final Logger LOG = Logger.getLogger(TasksPool.class);

    private TaskSettingsService taskSettingsService;

    /**
     * Конструктор на основе сервиса настроек задач по расписанию
     * 
     * @param taskSettingsService
     */
    public TasksPool(TaskSettingsService taskSettingsService) {
        this.taskSettingsService = taskSettingsService;
    }

    /**
     * Запускаем потоки задач
     */
    public void start() {
        try {
            Scheduler scheduler = new StdSchedulerFactory().getScheduler();
            scheduler.start();
            Map<Integer, TaskSettings> taskSettings = taskSettingsService.getTasks();
            for (TaskSettings task : taskSettings.values()) {
                if (task.getEnabled()) {
                    JobDetail jobDetail = JobBuilder.newJob(TaskRunner.class).build();
                    jobDetail.getJobDataMap().put("task", task);
                    Trigger trigger = null;
                    String cronString = task.getCronString();
                    if (cronString != null && !cronString.isEmpty()) {
                        trigger = TriggerBuilder.newTrigger()
                                .withSchedule(CronScheduleBuilder
                                        .cronSchedule(cronString)).build();
                    } else {
                        trigger = TriggerBuilder.newTrigger()
                                .startNow().build();
                    }

                    try {
                        scheduler.scheduleJob(jobDetail, trigger);
                    } catch (SchedulerException e) {
                        LOG.error(String.format("Ошибка при старте задачи task[id = %s]!", task.getTaskId()), e);
                    }
                }
            }
        } catch (SchedulerException e) {
            LOG.error("Ошибка при старте планировщика задач!", e);
        }
    }
}
