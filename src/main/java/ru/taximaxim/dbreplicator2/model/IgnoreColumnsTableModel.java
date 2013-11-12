package ru.taximaxim.dbreplicator2.model;

import javax.persistence.Column;
import javax.persistence.Id;

public class IgnoreColumnsTableModel {
    
    /**
     * Идентификатор таблицы
     */
    @Id
    @Column(name = "id_table")
    private Integer id;
    
    /**
     * Название колонки
     */
    @Column(name = "columnName")
    private String columnName;
    
    /**
     * Получение идентификатора таблицы
     * @return
     */
    public Integer getId() {
        return id;
    }
    
    /**
     * Получение имени колонки
     * @return
     */
    public String getColumnName() {
        return columnName;
    }
    
    /**
     * Установка идентификатора таблиц
     * @param id
     */
    public void setId(int id) {
        this.id = id;
    }
    
    /**
     * Получение имени колонки
     * @param columnName
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
