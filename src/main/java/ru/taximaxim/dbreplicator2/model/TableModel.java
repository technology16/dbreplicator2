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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;

import javax.persistence.*;

import ru.taximaxim.dbreplicator2.utils.Utils;

/**
 * Таблицы, обрабатываемые в пуле соединеий.
 *
 * @author volodin_aa
 *
 */
@Entity
@Table(name = "tables")
public class TableModel implements Cloneable, Serializable {
    
    private static final long serialVersionUID = 2L;
    
    /**
     * Название параметра списка игнорируемых колонок
     */
    public static final String IGNORED_COLUMNS = "ignoredCols";
    /**
     * Название параметра списка обязательных колонок
     */
    public static final String REQUIRED_COLUMNS = "requiredCols";
    /**
     * Название параметра имени таблицы в приемнике
     */
    public static final String DEST_TABLE_NAME = "dest";
    /**
     * Название параметра кастования колонки при извлечении данных 
     */
    public static final String CAST_FROM = "castfrom.";
    /**
     * Название параметра кастования колонки при репликации данных 
     */
    public static final String CAST_TO = "castto.";
    
    /**
     * Идентификатор таблицы
     */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "id_table")
    private Integer tableId;
    
    /**
     * Имя таблицы
     */
    @Column(name = "name")
    private String name;
    
    /**
     * Поток исполнитель, которому принадлежит настройка таблицы
     */
    @ManyToOne
    @JoinColumn(name = "id_runner")
    private RunnerModel runner;

    /**
     * Параметры таблицы для репликации
     */
    @Column(name = "param", length = 20000)
    private String param;

    /**
     * Поле для получения настроек
     */
    private Properties properties;

    /**
     * Получение идентификатора таблицы
     * 
     * @return
     */
    public Integer getTableId() {
        return tableId;
    }

    /**
     * Установка идентификатора таблицы
     * 
     * @param tableId
     */
    public void setTableId(Integer tableId) {
        this.tableId = tableId;
    }

    /**
     * Получение раннера, обрабатывающего таблицу
     * 
     * @return
     */
    public RunnerModel getRunner() {
        return runner;
    }

    /**
     * Установка раннера, обрабатывающего таблицу
     * 
     * @param runner
     */
    public void setRunner(RunnerModel runner) {
        this.runner = runner;
    }
    
    /**
     * Получение пула соединений, к которому принадлежит таблица
     * @return
     */
    public HikariCPSettingsModel getPool() {
        return runner.getSource();
    }

    /**
     * Получение имени таблицы
     * 
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * Установка имени таблицы
     * 
     * @param name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * Получение параметра по ключу
     */
    public String getParam(String key) {
        return getProperties().getProperty(key);
    }

    /**
     * Установка параметра таблицы
     *
     * @param key параметр
     * @param value значение
     */
    public void setParam(String key, String value) {
        getProperties().put(key, value);
        param = Utils.convertPropertiesToParamString(getProperties());
    }

    /**
     * Получение настроек
     *
     * @return
     */
    public Properties getProperties() {
        if (properties == null) {
            properties = Utils.convertParamStringToProperties(param);
        }
        return properties;
    }

    /**
     * @return копию текущего объект
     */
    public TableModel copy() {
        TableModel model = new TableModel();
        model.tableId = tableId;
        model.name = name;
        model.runner = runner;
        model.param = param;
        return model;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals()
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof TableModel)) {
            return false;
        }
        TableModel other = (TableModel) obj;
        return Objects.equals(name, other.name)
                && Objects.equals(runner, other.runner);
    }
    
    /**
     * Преобразование строки значений через запятую в список в верхнем регистре. 
     * При передачи null значения возвращается пустой список.
     * 
     * @param str  - список значений через запятую
     * @return
     */
    protected Collection<String> str2upperList(String str) {
        Collection<String> list = new ArrayList<>();
        if (str != null && !str.isEmpty()) {       
            list = Arrays.asList(str.toUpperCase().split(","));
        }
        return list;
    }
    
    /**
     * Получение списка игнорируемых колонок
     * @return
     */
    public Set<String> getIgnoredColumns(){
        Set<String> ingnoredColumns = new HashSet<>();
        if (getParam(IGNORED_COLUMNS) != null) {
            ingnoredColumns.addAll(str2upperList(getParam(IGNORED_COLUMNS)));
        }
        return ingnoredColumns;
    }
    
    /**
     * Получение списка колонок, обязательных к репликации
     * @return
     */
    public Set<String> getRequiredColumns(){
        Set<String> requiredColumns = new HashSet<>();
        if (getParam(REQUIRED_COLUMNS) != null) {
            requiredColumns.addAll(str2upperList(getParam(REQUIRED_COLUMNS)));
        }
        return requiredColumns;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((runner == null) ? 0 : runner.hashCode());
        return result;
    }
    
    /**
     * Получение наименование таблицы-приемника
     * 
     * @return
     */
    public String getDestTableName() {
        return getParam(DEST_TABLE_NAME);
    }
    
    /**
     * Возвращает map с кастованными полями для select запроса
     * @param data
     * @param columns
     * @return
     */
    public Map<String, String> getCastFromColumns(Collection<String> columns) {
        Map<String, String> castFromColums = new HashMap<>();
        for (String column : columns) {
            String castStatement = getParam(CAST_FROM + column.toLowerCase());
            if (castStatement != null) {
                castFromColums.put(column, castStatement);
            }
        }
        return castFromColums;
    }
    
    /**
     *  Возвращает map с кастованными полями для insert запроса
     * @param data
     * @param columns
     * @return
     */
    public Map<String, String> getCastToColumns(Collection<String> columns) {
        Map<String, String> castToColums = new HashMap<>();
        for (String column : columns) {
            String castStatement = getParam(CAST_TO + column.toLowerCase());
            if (castStatement != null) {
                castToColums.put(column, castStatement);
            }
        }
        return castToColums;
    }
}
