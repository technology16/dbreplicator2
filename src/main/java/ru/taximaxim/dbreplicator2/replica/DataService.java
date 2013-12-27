package ru.taximaxim.dbreplicator2.replica;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;

import ru.taximaxim.dbreplicator2.model.TableModel;

public interface DataService {

    /**
     * Инициализация с кешированием подготовленного запроса для удаления лог
     * записи из источника
     * 
     * @return the deleteDestStatements
     * @throws SQLException
     */
    public abstract PreparedStatement getDeleteStatement(TableModel table)
            throws SQLException;

    /**
     * Инициализация с кешированием подготовленного запроса для извлечения
     * записи из источника
     * 
     * @return the selectSourceStatements
     * @throws SQLException
     */
    public abstract PreparedStatement getSelectStatement(TableModel table)
            throws SQLException;

    /**
     * Инициализация с кешированием подготовленного запроса обновления записи в
     * приемнике
     * 
     * @return the updateDestStatements
     * @throws SQLException
     */
    public abstract PreparedStatement getUpdateStatement(TableModel table)
            throws SQLException;

    /**
     * Инициализация с кешированием подготовленного запроса для вставки записи в
     * приемник
     * 
     * @return the insertDestStatements
     * @throws SQLException
     */
    public abstract PreparedStatement getInsertStatement(TableModel table)
            throws SQLException;
    
    /**
     * Кешированное получение списка ключевых колонок
     * 
     * @param connection
     * @param table.getName()
     * @return
     * @throws SQLException
     */
    public Set<String> getPriCols(TableModel table) throws SQLException;


    /**
     * Кешированное получение списка всех колонок
     * 
     * @param connection
     * @param table.getName()
     * @return
     * @throws SQLException
     */
    public Set<String> getAllCols(TableModel table) throws SQLException;
    
    /**
     * Кешированное получение списка колонок с данными
     * 
     * @param connection
     * @param table.getName()
     * @return
     * @throws SQLException
     */
    public Set<String> getDataCols(TableModel table)
            throws SQLException;    
    
    /**
     * Кешированное получение списка автоинкрементных колонок
     * 
     * @param connection
     * @param table.getName()
     * @return
     * @throws SQLException
     */
    public Set<String> getIdentityCols(TableModel table) throws SQLException;
    
    /**
     * Кешированное получение списка игнорируемых колонок
     * 
     * @param connection
     * @param table.getName()
     * @return
     * @throws SQLException
     */
    public Set<String> getIgnoredCols(TableModel table) throws SQLException;

}