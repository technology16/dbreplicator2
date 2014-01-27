/* The MIT License (MIT)
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
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

/**
 * Персистентный класс игнорируемой колонки таблицы
 * 
 * @author volodin_aa
 *
 */
@Entity
@Table(name = "ignore_columns_table")
public class IgnoreColumnsTableModel {
    
    /**
     * Идентификатор
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    @Column(name = "id_ignore_columns_table")
    private Integer id;
    
    /**
     * Получение идентификатора
     * @return
     */
    public Integer getId() {
        return id;
    }
    
    /**
     * Установка идентификатора
     * @param id
     */
    public void setId(int id) {
        this.id = id;
    }
    
    /**
     * Название игнорируемой колонки
     */
    @Column(name = "column_name")
    private String columnName;

    /**
     * Получение название игнорируемой колонки
     * @return
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Получение название игнорируемой колонки
     * @param columnName
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    /**
     * Игнорируемая колонка, принадлежащей таблицы
     */
    @ManyToOne
    @JoinColumn(name = "id_table")
    private TableModel table;

    /**
     * @see TableModel#table
     */
    public TableModel getTable() {
        return this.table;
    }

    /**
     * @see TableModel#table
     */
    public void setTable(TableModel table) {
        this.table = table;
    }

}
