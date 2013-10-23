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

import java.util.Date;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.ThreadPool;
import ru.taximaxim.dbreplicator2.WorkerThread;
import ru.taximaxim.dbreplicator2.model.RunnerModel;

public class TaskThread implements Runnable {

    public static final Logger LOG = Logger.getLogger(TaskThread.class);
    
    private TaskSettings taskSettings;
    
    private RunnerModel runner;
    
    private boolean enabled;

    public TaskThread(TaskSettings taskSettings, RunnerModel runner) {
        this.taskSettings = taskSettings;
        this.runner = runner;
        
        enabled = true;
    }
    
    public void stop(){
        enabled = false;
    }

    @Override
    public void run() {
        LOG.info(String.format("Запуск задачи [%d] %s", 
                taskSettings.getTaskId(), taskSettings.getDescription()));
        while (enabled) {
            try {
                long startTime = new Date().getTime();

                new WorkerThread(runner).run();
                //threadPool.start(runner);

                // Ожидаем окончания периода синхронизации
                long sleepTime = startTime + taskSettings.getSuccessInterval() - new Date().getTime();

                if (sleepTime > 0) {
                    LOG.info(String.format("Ожидаем %d милисекунд после завершения задачи [%d] %s",
                            sleepTime, taskSettings.getTaskId(), taskSettings.getDescription()));
                    Thread.sleep(sleepTime);
                }
            } catch (Exception e) {
                LOG.error(String.format("Ошибка при выполнении задачи [%d] %s", 
                        taskSettings.getTaskId(), taskSettings.getDescription()), e);

                LOG.info(String.format("Ожидаем %d милисекунд до рестарта задачи [%d] %s", 
                        taskSettings.getFailInterval(), taskSettings.getTaskId(), taskSettings.getDescription()));
                try {
                    Thread.sleep(taskSettings.getFailInterval());
                } catch (InterruptedException ex) {
                    LOG.warn(ex);
                }
            }
        }
    }
}
