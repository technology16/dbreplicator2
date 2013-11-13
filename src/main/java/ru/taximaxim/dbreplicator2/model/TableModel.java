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

import java.util.ArrayList;
import java.util.List;

import javax.persistence.CascadeType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.JoinTable;
import javax.persistence.ManyToMany;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

/**
 * Таблицы, обрабатываемые в пуле соединеий.
 * 
 * 
 * @author volodin_aa
 *
 */
@Entity
@Table(name = "tables")
public class TableModel {

    /**
     * Идентификатор таблицы
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    @Column(name = "id_table")
    private Integer tableId;

    public Integer getTableId() {
        return tableId;
    }

    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }

    /**
     * Пул (соответственно БД), которому принадлежит таблица
     */
    @ManyToOne
    @JoinColumn(name = "id_pool")
    private BoneCPSettingsModel pool;

    public BoneCPSettingsModel getPool() {
        return pool;
    }

    public void setPool(BoneCPSettingsModel pool) {
        this.pool = pool;
    }

    /**
     * Имя таблицы
     */
    @Column(name = "name")
    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * Список раннеров, обрабатывающих таблицу
     */
    @ManyToMany(fetch = FetchType.EAGER, cascade = CascadeType.ALL)
    @JoinTable(name = "table_observers", joinColumns = { 
            @JoinColumn(name = "id_table") }, 
            inverseJoinColumns = { @JoinColumn(name = "id_runner") })
    @Fetch(FetchMode.SELECT)
    private List<RunnerModel> runners;
    
    public List<RunnerModel> getRunners() {
        if (runners == null) {
            runners = new ArrayList<RunnerModel>();
        }
        return this.runners;
    }
 
    public void setRunners(List<RunnerModel> runners) {
        this.runners = runners;
    }

    /**
     * Список игнорируемых колонок
     */
    @OneToMany(mappedBy = "table", fetch = FetchType.EAGER)
    @Fetch(FetchMode.SELECT)
    private List<IgnoreColumnsTableModel> ignoreColumnsTable;
    
    /**
     * Получения списка игнорируеммых колонок
     * @return
     */
    public List<IgnoreColumnsTableModel> getIgnoreColumnsTable() {
        if (ignoreColumnsTable == null) {
            ignoreColumnsTable = new ArrayList<IgnoreColumnsTableModel>();
        }
        return ignoreColumnsTable;
    }

    /**
     * Установка списка игнорируемых колонок
     * @param ignoreColumnsTable
     */
    public void setIgnoreColumnsTable(List<IgnoreColumnsTableModel> ignoreColumnsTable) {
        this.ignoreColumnsTable = ignoreColumnsTable;
    }
}
