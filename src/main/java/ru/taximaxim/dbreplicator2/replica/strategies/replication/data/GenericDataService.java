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

package ru.taximaxim.dbreplicator2.replica.strategies.replication.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.jdbc.QueryConstructors;
import ru.taximaxim.dbreplicator2.model.IgnoreColumnsTableModel;
import ru.taximaxim.dbreplicator2.model.TableModel;

/**
 * @author volodin_aa
 *
 */
public class GenericDataService implements DataService {

    private Connection connection;

    /**
     * @return the connection
     */
    protected Connection getConnection() {
        return connection;
    }

    /**
     * Кешированные запросы удаления данных в приемнике
     */
    private Map<TableModel, PreparedStatement> deleteStatements = new HashMap<TableModel, PreparedStatement>();
    /**
     * Кешированные запросы получения данных из источника
     */
    private Map<TableModel, PreparedStatement> selectStatements = new HashMap<TableModel, PreparedStatement>();
    /**
     * Кешированные запросы обновления данных в приемнике
     */
    private Map<TableModel, PreparedStatement> updateStatements = new HashMap<TableModel, PreparedStatement>();
    /**
     * Кешировнные запросы вставки данных в приемник
     */
    private Map<TableModel, PreparedStatement> insertStatements = new HashMap<TableModel, PreparedStatement>();

    private Map<TableModel, Set<String>> priCols = new HashMap<TableModel, Set<String>>();
    private Map<TableModel, Set<String>> allCols = new HashMap<TableModel, Set<String>>();
    private Map<TableModel, Set<String>> dataCols = new HashMap<TableModel, Set<String>>();
    private Map<TableModel, Set<String>> identityCols = new HashMap<TableModel, Set<String>>();
    private Map<TableModel, Set<String>> ignoredCols = new HashMap<TableModel, Set<String>>();
    
    /**
     * 
     */
    public GenericDataService(Connection connection) {
        this.connection = connection;        
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.DataService2#getDeleteStatement(ru.taximaxim.dbreplicator2.model.TableModel)
     */
    @Override
    public PreparedStatement getDeleteStatement(TableModel table) throws SQLException {
        PreparedStatement statement = getDeleteStatements().get(table);
        if (statement == null) {
            statement = connection.prepareStatement(QueryConstructors
                    .constructDeleteQuery(table.getName(),
                            new ArrayList<String>(getPriCols(table))));
            getDeleteStatements().put(table, statement);
        }

        return statement;
    }

    /**
     * @return the deleteStatements
     */
    protected Map<TableModel, PreparedStatement> getDeleteStatements() {
        return deleteStatements;
    }

    /**
     * Получение кеша запросов на выборку данных из источника
     * 
     * @return the selectStatements
     */
    protected Map<TableModel, PreparedStatement> getSelectStatements() {
        return selectStatements;
    }

    /**
     * @return the updateStatements
     */
    protected Map<TableModel, PreparedStatement> getUpdateStatements() {
        return updateStatements;
    }

    /**
     * Получение кеша запросов на вставку данных в приемник
     * 
     * @return the insertStatements
     */
    protected Map<TableModel, PreparedStatement> getInsertStatements() {
        return insertStatements;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.DataService2#getSelectStatement(ru.taximaxim.dbreplicator2.model.TableModel)
     */
    @Override
    public PreparedStatement getSelectStatement(TableModel table) throws SQLException {
        PreparedStatement statement = getSelectStatements().get(table);
        if (statement == null) {
            statement = connection.prepareStatement(QueryConstructors
                    .constructSelectQuery(table.getName(),
                            new ArrayList<String>(getAllCols(table)),
                                    new ArrayList<String>(getPriCols(table))));

            getSelectStatements().put(table, statement);
        }

        return statement;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.DataService2#getUpdateStatement(ru.taximaxim.dbreplicator2.model.TableModel)
     */
    @Override
    public PreparedStatement getUpdateStatement(TableModel table) throws SQLException {
        PreparedStatement statement = getUpdateStatements().get(table);
        if (statement == null) {
            statement = connection.prepareStatement(QueryConstructors
                    .constructUpdateQuery(table.getName(),
                            new ArrayList<String>(getDataCols(table)),
                                    new ArrayList<String>(getPriCols(table))));

            getUpdateStatements().put(table, statement);
        }

        return statement;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.DataService2#getInsertStatement(ru.taximaxim.dbreplicator2.model.TableModel)
     */
    @Override
    public PreparedStatement getInsertStatement(TableModel table) throws SQLException {
        PreparedStatement statement = getInsertStatements().get(table);
        if (statement == null) {
            String insertQuery = QueryConstructors.constructInsertQuery(table.getName(), 
                    new ArrayList<String>(getAllCols(table)));
            statement = connection.prepareStatement(insertQuery);

            getInsertStatements().put(table, statement);
        }

        return statement;
    }

    /**
     * Кешированное получение списка ключевых колонок
     * 
     * @param connection
     * @param table.getName()
     * @return
     * @throws SQLException
     */
    public Set<String> getPriCols(TableModel table)
            throws SQLException {
        Set<String> cols = priCols.get(table);
        if (cols == null) {
            cols = JdbcMetadata.getPrimaryColumns(connection, table.getName());
            priCols.put(table, cols);
        }

        return cols;
    }

    /**
     * Кешированное получение списка всех колонок
     * 
     * @param connection
     * @param table.getName()
     * @return
     * @throws SQLException
     */
    public Set<String> getAllCols(TableModel table)
            throws SQLException {
        Set<String> cols = allCols.get(table);
        if (cols == null) {
            cols = JdbcMetadata.getColumns(connection, table.getName());
            
            // Удаляем игнорируемые колонки
            for (String ignoredCol: getIgnoredCols(table)) {
                cols.remove(ignoredCol.toUpperCase());
            }
            
            allCols.put(table, cols);
        }

        return cols;
    }

    /**
     * Кешированное получение списка колонок с данными
     * 
     * @param connection
     * @param table.getName()
     * @return
     * @throws SQLException
     */
    public Set<String> getDataCols(TableModel table)
            throws SQLException {
        Set<String> cols = dataCols.get(table);
        if (cols == null) {
            Set<String> priColsList = getPriCols(table);
            Set<String> allColsList = getAllCols(table);
            cols = new HashSet<String>(allColsList);
            for (String col : priColsList) {
                cols.remove(col);
            }

            dataCols.put(table, cols);
        }

        return cols;
    }

    /**
     * Кешированное получение списка автоинкрементных колонок
     * 
     * @param connection
     * @param table.getName()
     * @return
     * @throws SQLException
     */
    public Set<String> getIdentityCols(TableModel table) throws SQLException {
        Set<String> cols = identityCols.get(table);
        if (cols == null) {
            cols = JdbcMetadata.getIdentityColumns(connection, table.getName());
            identityCols.put(table, cols);
        }

        return cols;
    }

    /**
     * Кешированное получение списка игнорируемых колонок
     * 
     * @param connection
     * @param table.getName()
     * @return
     * @throws SQLException
     */
    public Set<String> getIgnoredCols(TableModel table) throws SQLException {
        Set<String> cols = ignoredCols.get(table);
        if (cols == null) {
            cols = new HashSet<String>();
            for (IgnoreColumnsTableModel ignoredColumn: table.getIgnoreColumnsTable()) {
                cols.add(ignoredColumn.getColumnName());
            }
            ignoredCols.put(table, cols);
        }

        return cols;
    }

    /**
     * Устанавливает сессионную переменную с именем текущей подписки или
     * публикации
     * 
     * @throws Exception
     */
    protected void setRepServerName(Connection connection, String repServerName)
            throws SQLException {

    }
    
}
