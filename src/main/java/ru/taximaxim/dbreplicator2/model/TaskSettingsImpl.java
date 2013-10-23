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
package ru.taximaxim.dbreplicator2.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import ru.taximaxim.dbreplicator2.replica.Runner;
import ru.taximaxim.dbreplicator2.tasks.TaskSettings;

/**
 * Класс инкапсулирующий задачу менеджера записей.
 * 
 * @author ags
 *
 */
@Entity
@Table( name = "tasks" )
public class TaskSettingsImpl implements TaskSettings{
    
    /**
     * Идентификатор задачи
     */
    private int taskId;
    
    /**
     * Идентификатор реплики
     */
    private int runnerId;
    
    /**
     * Флаг доступности задачи
     */
    private boolean enabled;
    
    /**
     * Интервал после успешного выполнения задачи, мс
     */
    private int successInterval;
    
    /**
     * Интервал после ошибочного выполнения задачи
     */
    private int failInterval;
    
    /**
     * Интервал после ошибочного выполнения задачи
     */
    private String description;
    
    /**
     * Инициализированный обработчик реплики. Будет инициализироваться сервисом хранения настроек задач.
     */
    private Runner runner;

	/**
	 * Для использования выполнения задачи будем использовать поток реплику, как
	 * подготовленное рабочее решение.
	 * 
	 */

    @Id
    @Column(name = "task_id")
    @Override
    public int getTaskId() {
        return taskId;
    }

    @Override
    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    @Column(name = "runner_id")
    @Override
    public int getRunnerId() {
        return runnerId;
    }

    @Override
    public void setRunnerId(int runnerId) {
        this.runnerId = runnerId;
    }

    @Column(name = "enabled")
    @Override
    public boolean getEnabled() {
        return enabled;
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Column(name = "success_interval")
    @Override
    public int getSuccessInterval() {
        return successInterval;
    }

    @Override
    public void setSuccessInterval(int successInterval) {
        this.successInterval = successInterval;
    }

    @Column(name = "fail_interval")
    @Override
    public int getFailInterval() {
        return failInterval;
    }

    @Override
    public void setFailInterval(int failInterval) {
        this.failInterval = failInterval;
    }

    @Column(name = "description")
    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public Runner getRunner() {
        return runner;
    }

    @Override
    public void setRunner(Runner runner) {
        this.runner = runner;
    }
}
