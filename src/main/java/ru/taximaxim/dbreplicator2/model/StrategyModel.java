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
import java.io.StringReader;
import java.util.Properties;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.apache.log4j.Logger;

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
    @Column(length=2000)
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
     * Поток исполнитель, которому принадлежит стратегия
     */
    @ManyToOne
    @JoinColumn(name = "id_runner")
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
    public String getParam(String key) {
        return getProp().getProperty(key);
    }

    /**
     * @see StrategyModel#param
     */
    public void setParam(String key, String value) {
        
        String paramstr = null;
        getProp().put(key, value);
        Properties prop = getProp();
        if(key != null) {
            paramstr = "";
            for (String p : prop.stringPropertyNames()) {
                paramstr += String.format("%s=%s|",p, prop.getProperty(p));
            }
            paramstr = paramstr.substring(0, paramstr.length() - 1);
        }
        
        this.param = paramstr;
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
                    String[] properties = param.split("\\|");
                    for (String p : properties) {
                        this.prop.load(new StringReader(p.replace("\"", "'")));
                    }
                } catch (IOException e) {
                    Logger.getLogger("StrategyModel").error("Ошибка при чтение параметров [" + param + "]!", e);
                }
            }
        }
        return this.prop;
    }
}
