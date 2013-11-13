package ru.taximaxim.dbreplicator2.model;

import java.util.ArrayList;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.OneToMany;
import javax.persistence.Table;

@Entity
@Table(name = "ignore_columns_table")
public class IgnoreColumnsTableModel {
    
    /**
     * Идентификатор
     */
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE)
    @Column(name = "id")
    private Integer id;

    /**
     * Идентификатор таблицы
     */
    @Column(name = "id_table")
    private Integer idTable;
    
    /**
     * Название колонки
     */
    @Column(name = "column_name")
    private String columnName;
    
    /**
     * Поток исполнитель, которому принадлежит стратегия
     */
//    @OneToMany
//    @JoinColumn(name = "id_table")
//    private List<TableModel> tableModelList;

    @ManyToOne
    @JoinColumn(name = "id_table")
    private TableModel tableModel;
    
    
    
    /**
     * Получение идентификатора
     * @return
     */
    public Integer getId() {
        return id;
    }

    /**
     * Получение идентификатора таблиц
     * @return
     */
    public Integer getIdTable() {
        return idTable;
    }
    
    /**
     * Получение имени колонки
     * @return
     */
    public String getColumnName() {
        return columnName;
    }
    
    /**
     * @see TableModel#tableModelList
     */
//    public List<TableModel> getTableModel() {
//        if (tableModelList == null) {
//            tableModelList = new ArrayList<TableModel>();
//        }
//        return this.tableModelList;
//    }
    
    public TableModel getTableModel() {
        return this.tableModel;
    }
    
    
    /**
     * Установка идентификатора
     * @param id
     */
    public void setId(int id) {
        this.id = id;
    }
    
    /**
     * Установка идентификатора таблиц
     * @param IdTable
     */
    public void setIdTable(int idTable) {
        this.idTable = idTable;
    }
    
    /**
     * Получение имени колонки
     * @param columnName
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    /**
     * @see TableModel#tableModelList
     */
//    public void setTableModel(List<TableModel> tableModelList) {
//        this.tableModelList = tableModelList;
//    }
    public void setTableModel(TableModel tableModel) {
        this.tableModel = tableModel;
    }
    
    
}
