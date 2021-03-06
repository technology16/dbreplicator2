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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.persistence.*;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

/**
 * Персистентная реализация интерфейс раннера
 * 
 * @author volodin_aa
 *
 */
@Entity
@Table(name = "runners")
public class RunnerModel implements Runner, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Обработчики реплики
     */
    public static final String REPLICA_RUNNER_CLASS = 
            "ru.taximaxim.dbreplicator2.replica.ReplicaRunner";
    
    /**
     * Менеджеры записей суперлога
     */
    public static final String SUPERLOG_RUNNER_CLASS = 
            "ru.taximaxim.dbreplicator2.replica.SuperlogRunner";
    

    /**
     * Идентификатор выполняемого потока
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    @Column(name = "id_runner")
    private Integer id;

    /**
     * Именованный пул-источник
     */
    @ManyToOne
    @JoinColumn(name = "source")
    private HikariCPSettingsModel source;

    /**
     * Именованный целевой пул
     */
    @ManyToOne
    @JoinColumn(name = "target")
    private HikariCPSettingsModel target;

    /**
     * Описание потока исполнителя
     */
    private String description;

    /**
     * Список стратегий, которые необхоимо выполнить потоку
     */
    @OneToMany(mappedBy = "runner", fetch = FetchType.EAGER)
    @Fetch(FetchMode.SELECT)
    @OrderBy("priority ASC")
    private List<StrategyModel> strategyModels;
    
    /**
     * Список таблиц, которые обрабатывает раннер
     */
    @OneToMany(mappedBy = "runner", fetch = FetchType.EAGER)
    @Fetch(FetchMode.SELECT)
    @OrderBy("id_table ASC")
    private List<TableModel> tables;

    /**
     * Добавляет стратегию к runner'y
     * 
     * @param strategy
     * @return
     */
    public List<StrategyModel> addStrategy(StrategyModel strategy) {

        List<StrategyModel> strategies = getStrategyModels();
        strategies.add(strategy);
        strategy.setRunner(this);

        return strategies;
    }

    @Override
    public HikariCPSettingsModel getSource() {
        return source;
    }

    @Override
    public HikariCPSettingsModel getTarget() {
        return target;
    }

    @Override
    public Integer getId() {
        return id;
    }

    @Override
    public String getDescription() {
        return description;
    }

    /**
     * Получение списка стратегий раннера
     */
    @Override
    public List<StrategyModel> getStrategyModels() {

        if (strategyModels == null) {
            strategyModels = new ArrayList<>();
        }

        return strategyModels;
    }

    /**
     * Установка списка стратегий раннера
     * 
     * @param strategyModels
     */
    public void setStrategyModels(List<StrategyModel> strategyModels) {
        this.strategyModels = strategyModels;
    }

    /**
     * Изменение идентификатора раннера
     * 
     * @param id новый идентификатор
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Изменение источника данных раннера
     * 
     * @param source источник
     */
    public void setSource(HikariCPSettingsModel source) {
        this.source = source;
    }

    /**
     * Изменение приемника данных
     * 
     * @param target приемник
     */
    public void setTarget(HikariCPSettingsModel target) {
        this.target = target;
    }

    /**
     * Изменение описания раннера
     * 
     * @param description описание
     */
    public void setDescription(String description) {
        this.description = description;
    }
    
    /**
     * Получение списка обрабатываемых раннером таблиц
     * 
     * @return список таблиц
     */
    public List<TableModel> getTables() {
        if (tables == null) {
            tables = new ArrayList<>();
        }
        return this.tables;
    }
    
    /**
     * Получение таблицы tableName, обрабатываемую раннером
     * 
     * @return список таблиц
     */
    public TableModel getTable(String tableName) {
        for (TableModel table : tables) {
            if (table.getName().equalsIgnoreCase(tableName)) {
                return table;
            }
        }
        return null;
    }
    
    /**
     * Изменнение списка обрабатываемых раннером таблиц
     * 
     * @param tables список таблиц
     */
    public void setTables(List<TableModel> tables) {
        this.tables = tables;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return "RunnerModel [id=" + id + ", source=" + source.getPoolId() + ", target=" + target.getPoolId()
                + ", description=" + description + "]";
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#equals()
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof RunnerModel)) {
            return false;
        }
        RunnerModel other = (RunnerModel) obj;
        return Objects.equals(id, other.id);
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 10;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }
}
