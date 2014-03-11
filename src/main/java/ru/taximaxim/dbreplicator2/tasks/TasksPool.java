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

import java.util.HashMap;
import java.util.Map;

import ru.taximaxim.dbreplicator2.el.DefaultUncaughtExceptionHandler;
import ru.taximaxim.dbreplicator2.model.TaskSettings;
import ru.taximaxim.dbreplicator2.model.TaskSettingsService;

/**
 * Пул задач по расписанию
 * 
 * @author volodin_aa
 * 
 */
public class TasksPool {

    private Map<TaskRunner, Thread> taskThreads = new HashMap<TaskRunner, Thread>();

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
        Map<Integer, TaskSettings> taskSettings = taskSettingsService.getTasks();

        for (TaskSettings task : taskSettings.values()) {
            if (task.getEnabled()) {
                TaskRunner taskRunner = new TaskRunner(task);
                Thread thread = new Thread(taskRunner);
                thread.setUncaughtExceptionHandler(
                        new DefaultUncaughtExceptionHandler(task.getRunner().getId()));
                thread.start();
                taskThreads.put(taskRunner, thread);
            }
        }
    }

    /**
     * Останливаем потоки задач
     * 
     * @throws InterruptedException
     */
    public void stop() throws InterruptedException {
        // Сигнализируем обработчикам задач что надо остановиться
        for (TaskRunner taskRunner : taskThreads.keySet()) {
            taskRunner.stop();
        }

        // Дожидаемся завершения потоков задач
        for (TaskRunner taskRunner : taskThreads.keySet()) {
            taskThreads.get(taskRunner).join();
        }

        taskThreads.clear();
    }
}
