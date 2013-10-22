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

import ru.taximaxim.dbreplicator2.tasks.TaskSettings;

/**
 * Класс инкапсулирующий задачу менеджера записей.
 * 
 * @author ags
 *
 */
@Entity
@Table( name = "task_settings" )
public class TaskSettingsImpl implements TaskSettings{
    
    /**
     * Идентификатор задачи
     */
    private int taskId;
    
    /**
     * Идентификатор реплики
     */
    private int replicaId;
    
    /**
     * Приоритет, задачи выполняются в порядке возрастания приоритета
     */
    private int priority;
    
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
	 * Для использования выполнения задачи будем использовать поток реплику, как
	 * подготовленное рабочее решение.
	 * 
	 */
	/*private ReplicaRunner runner;
	
	public ReplicaRunner getRunner() {
		return runner;
	}
	*/

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.tasks.TaskSettings#getTaskId()
     */
    @Id
    @Column(name = "task_id")
    @Override
    public int getTaskId() {
        // TODO Auto-generated method stub
        return taskId;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.tasks.TaskSettings#setTaskId(int)
     */
    @Override
    public void setTaskId(int taskId) {
        // TODO Auto-generated method stub
        this.taskId = taskId;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.tasks.TaskSettings#getReplicaId()
     */
    @Column(name = "replica_id")
    @Override
    public int getReplicaId() {
        // TODO Auto-generated method stub
        return replicaId;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.tasks.TaskSettings#setReplicaId(int)
     */
    @Override
    public void setReplicaId(int replicaId) {
        // TODO Auto-generated method stub
        this.replicaId = replicaId;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.tasks.TaskSettings#getPriority()
     */
    @Column(name = "priority")
    @Override
    public int getPriority() {
        // TODO Auto-generated method stub
        return priority;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.tasks.TaskSettings#setPriority(int)
     */
    @Override
    public void setPriority(int priority) {
        // TODO Auto-generated method stub
        this.priority = priority;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.tasks.TaskSettings#getEnabled()
     */
    @Column(name = "enabled")
    @Override
    public boolean getEnabled() {
        // TODO Auto-generated method stub
        return enabled;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.tasks.TaskSettings#setEnabled(boolean)
     */
    @Override
    public void setEnabled(boolean enabled) {
        // TODO Auto-generated method stub
        this.enabled = enabled;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.tasks.TaskSettings#getSuccessInterval()
     */
    @Column(name = "success_interval")
    @Override
    public int getSuccessInterval() {
        // TODO Auto-generated method stub
        return successInterval;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.tasks.TaskSettings#setSuccessInterval(int)
     */
    @Override
    public void setSuccessInterval(int successInterval) {
        // TODO Auto-generated method stub
        this.successInterval = successInterval;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.tasks.TaskSettings#getFailInterval()
     */
    @Column(name = "fail_interval")
    @Override
    public int getFailInterval() {
        // TODO Auto-generated method stub
        return failInterval;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.tasks.TaskSettings#setFailInterval(int)
     */
    @Override
    public void setFailInterval(int failInterval) {
        // TODO Auto-generated method stub
        this.failInterval = failInterval;
    }
	
}
