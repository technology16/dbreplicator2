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

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

@Entity
@Table(name = "strategies")
public class StrategyModel {

    /**
     * Идентификатор стратегии
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    private Integer id;

    /**
     * Имя класса
     */
    private String className;

    /**
     * Параметр
     */
    private String param;

    /**
     * Является ли стратегия рабочей на текущий момент времени
     */
    private boolean isEnabled;

    /**
     * Приоритет стратегии, чем меньше, тем выше.
     */
    private int priority;

    /**
     * Поток исполнитель, которому принадлежит стратегия
     */
    @ManyToOne
    @JoinColumn(name = "runner_id")
    private RunnerModel runner;

    /**
     * @see StrategyModel#runner
     */
    public RunnerModel getRunner() {
        return runner;
    }

    /**
     * @see StrategyModel#runner
     */
    public void setRunner(RunnerModel runner) {
        this.runner = runner;
    }

    /**
     * @see StrategyModel#id
     */
    public Integer getId() {
        return id;
    }

    /**
     * @see StrategyModel#id
     */
    public void setId(Integer id) {
        this.id = id;
    }

    /**
     * @see StrategyModel#className
     */
    public String getClassName() {
        return className;
    }

    /**
     * @see StrategyModel#className
     */
    public void setClassName(String className) {
        this.className = className;
    }

    /**
     * @see StrategyModel#param
     */
    public String getParam() {
        return param;
    }

    /**
     * @see StrategyModel#param
     */
    public void setParam(String param) {
        this.param = param;
    }

    /**
     * @see StrategyModel#isEnabled
     */
    public boolean isEnabled() {
        return isEnabled;
    }

    /**
     * @see StrategyModel#isEnabled
     */
    public void setEnabled(boolean isEnabled) {
        this.isEnabled = isEnabled;
    }

    /**
     * @see StrategyModel#priority
     */
    public int getPriority() {
        return priority;
    }

    /**
     * @see StrategyModel#priority
     */
    public void setPriority(int priority) {
        this.priority = priority;
    }
}
