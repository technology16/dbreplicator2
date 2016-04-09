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
import java.io.StringWriter;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.persistence.*;

import org.apache.log4j.Logger;

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
        StringWriter writer = new StringWriter();
        properties.list(new PrintWriter(writer));
        this.param =  writer.getBuffer().toString();
    }

    /**
     * Получение настроек
     * 
     * @return
     */
    public Properties getProperties() {
        if(properties == null) {
            properties = new Properties();
            if(param != null){
                try {
                    properties.load(new StringReader(param));
                } catch (IOException e) {
                    Logger.getLogger("TableModel").error("Ошибка при чтение параметров [" + param + "]!", e);
                }
            }
        }
        return properties;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#clone()
     */
    @Override
    public Object clone() throws CloneNotSupportedException {
        TableModel clone;
        clone = (TableModel) super.clone();
        if (properties != null) {
            clone.properties = (Properties) properties.clone();
        }
        
        return clone;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#equals()
     */
    @Override
    public boolean equals(Object object) {
        if ((object == null) || !(object instanceof TableModel)) {
            return false;
        }
        TableModel table = (TableModel) object;
        if (this.getName().equals(table.getName())) {
            return true;
        }
        return false;
    }
    
    /**
     * Преобразование строки значений через запятую в список в верхнем регистре. 
     * При передачи null значения возвращается пустой список.
     * 
     * @param str  - список значений через запятую
     * @return
     */
    protected Collection<String> str2upperList(String str) {
        Collection<String> list = new ArrayList<String>();
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
        Set<String> ingnoredColumns = new HashSet<String>();
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
        Set<String> requiredColumns = new HashSet<String>();
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
        result = prime * result + ((param == null) ? 0 : param.hashCode());
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
     * @throws SQLException 
     */
    public Map<String, String> getCastFromColumns(Collection<String> columns) throws SQLException {
        Map<String, String> castFromColums = new HashMap<String, String>();
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
     * @throws SQLException 
     */
    public Map<String, String> getCastToColumns(Collection<String> columns) throws SQLException {
        Map<String, String> castToColums = new HashMap<String, String>();
        for (String column : columns) {
            String castStatement = getParam(CAST_TO + column.toLowerCase());
            if (castStatement != null) {
                castToColums.put(column, castStatement);
            }
        }
        return castToColums;
    }
}
