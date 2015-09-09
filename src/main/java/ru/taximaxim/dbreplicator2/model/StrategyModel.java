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

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Properties;

import javax.persistence.*;

import org.apache.log4j.Logger;

/**
 * Персистентный класс настроек стратегии
 * 
 * @author volodin_aa
 *
 */
@Entity
@Table(name = "strategies")
@IdClass(StrategyKey.class)
public class StrategyModel implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    public StrategyModel() {}
    
    public StrategyModel(StrategyKey key) {
        this.id = key.getId();
        this.runner = key.getRunner();
    }
    
    /**
     * Идентификатор стратегии
     */
    /**
     * Идентификатор настройки
     */
    @Id
    @AttributeOverrides({
        @AttributeOverride(name = "id", column = @Column(name = "id")),
        @AttributeOverride(name = "runner", column = @Column(name = "id_runner"))
    })

    @Column(name = "id")
    private Integer id;
    
    /**
     * Поток исполнитель, которому принадлежит стратегия
     */
    @ManyToOne
    @JoinColumn(name = "id_runner")
    private RunnerModel runner;
    
    /**
     * Имя класса
     */
    private String className;

    /**
     * Параметры
     */
    @Column(length=20000)
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
     * Настройки
    */
    private Properties prop;

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
     * Получение параметра по ключу
     */
    public String getParam(String key) {
        return getProp().getProperty(key);
    }

    /**
     * Запись параметра по ключу
     */
    public void setParam(String key, String value) {
        getProp().put(key, value);
        
        StringWriter writer = new StringWriter();
        prop.list(new PrintWriter(writer));
        this.param =  writer.getBuffer().toString();
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
    
    /**
     * Получение настроек
     * @return
     */
    private Properties getProp(){
        if(this.prop == null) {
            this.prop = new Properties();
            if(param != null){
                try {
                    this.prop.load(new StringReader(param));
                } catch (IOException e) {
                    Logger.getLogger("StrategyModel").error("Ошибка при чтение параметров [" + param + "]!", e);
                }
            }
        }
        return this.prop;
    }
}
