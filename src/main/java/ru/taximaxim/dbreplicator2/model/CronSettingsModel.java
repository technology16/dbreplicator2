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
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;


/**
 * Класс инкапсулирующий задачу менеджера записей.
 *
 * @author ags
 *
 */
@Entity
@Table( name = "cron" )
public class CronSettingsModel implements TaskSettings{

    /**
     * Идентификатор задачи
     */
    @Id
    @Column(name = "id_task")
    private int taskId;

    /**
     * Флаг доступности задачи
     */
    @Column(name = "enabled")
    private boolean enabled;

    /**
     * Cron строка, описывающая периодичность запуска задачи.
     * (В случае использования simple триггера будет равно null)
     */
    @Column(name = "cron_string")
    private String cronString;

    /**
     * Интервал после ошибочного выполнения задачи
     */
    @Column(name = "description")
    private String description;

    /**
     * Обработчик реплики
     */
    @ManyToOne
    @JoinColumn(name = "id_runner")
    @Fetch(FetchMode.SELECT)
    private RunnerModel runner;

    /**
     * Для использования выполнения задачи будем использовать поток реплику, как
     * подготовленное рабочее решение.
     *
     */

    @Override
    public int getTaskId() {
        return taskId;
    }

    @Override
    public void setTaskId(int taskId) {
        this.taskId = taskId;
    }

    @Override
    public boolean getEnabled() {
        return enabled;
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

     @Override
    public String getCronString() {
        return cronString;
    }

    @Override
    public void setCronString(String cronString) {
        this.cronString = cronString;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public void setDescription(String description) {
        this.description = description;
    }

    @Override
    public RunnerModel getRunner() {
        return runner;
    }

    @Override
    public void setRunner(RunnerModel runner) {
        this.runner = runner;
    }
}
