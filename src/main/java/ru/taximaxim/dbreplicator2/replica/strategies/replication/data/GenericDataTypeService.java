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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.jdbc.QueryConstructors;
import ru.taximaxim.dbreplicator2.model.TableModel;

/**
 * @author volodin_aa
 * 
 */
public class GenericDataTypeService extends GenericDataService implements DataService {
    /**
     * Кешированные запросы получения данных из источника и приемника
     */
    private Map<TableModel, PreparedStatement> selectStatementsAll = new HashMap<TableModel, PreparedStatement>();    
    private Map<TableModel, Map<String, Integer>> allColsTypes = new HashMap<TableModel, Map<String, Integer>>();
    private Map<TableModel, Map<String, Integer>> priColsTypes = new HashMap<TableModel, Map<String, Integer>>();
    
    /**
     * 
     */
    public GenericDataTypeService(Connection connection) {
        super(connection);        
    }
    
    /**
     * Получение кеша запросов на выборку данных из источника и приемника
     * 
     * @return the selectStatements
     */
    protected Map<TableModel, PreparedStatement> getSelectStatementsAll() {
        return selectStatementsAll;
    }
    
    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.DataService2#getSelectStatement(ru.taximaxim.dbreplicator2.model.TableModel)
     */
    
    public PreparedStatement getSelectStatement(TableModel table) throws SQLException {
        PreparedStatement statement = getSelectStatements().get(table);
        if (statement == null) {
            statement = getConnection().prepareStatement(QueryConstructors
                    .constructSelectQuery(table.getName(),
                    new ArrayList<String>(getAllCols(table)),
                    new ArrayList<String>(getPriCols(table))));
            getSelectStatements().put(table, statement);
        }
        return statement;
    }
    
    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.DataService2#getSelectStatementAll(ru.taximaxim.dbreplicator2.model.TableModel)
     */
    public PreparedStatement getSelectStatementAll(TableModel table) throws SQLException {
        PreparedStatement statement = getSelectStatementsAll().get(table);
        if (statement == null) {
            statement = getConnection().prepareStatement(QueryConstructors.
                constructSelectQuery(table.getName(),
                new ArrayList<String>(getAllCols(table)), null, 
                new ArrayList<String>(getPriCols(table))),
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            getSelectStatementsAll().put(table, statement);
        }
        return statement;
    }

    /**
     * Кешированное получение списка всех колонок
     * 
     * @param connection
     * @param table.getName()
     * @return
     * @throws SQLException
     */
    public Map<String, Integer> getAllColsTypes(TableModel table) throws SQLException {
        Map<String, Integer> colsTypes = allColsTypes.get(table);
        if (colsTypes == null) {
            colsTypes = JdbcMetadata.getColumnsTypes(getConnection(), table.getName());
            // Удаляем игнорируемые колонки
            for (String ignoredCol: getIgnoredCols(table)) {
                colsTypes.remove(ignoredCol.toUpperCase());
            }
            allColsTypes.put(table, colsTypes);
        }
        return colsTypes;
    }
    
    /**
     * Кешированное получение списка ключевых колонок
     * 
     * @param connection
     * @param table.getName()
     * @return
     * @throws SQLException
     */
    public Map<String, Integer> getPriColsTypes(TableModel table)
            throws SQLException {
        Map<String, Integer> colsTypes = priColsTypes.get(table);
        if (colsTypes == null) {
            colsTypes = JdbcMetadata.getPrimaryColumnsTypes(getConnection(), table.getName());
            priColsTypes.put(table, colsTypes);
        }
        return colsTypes;
    }
    
    @Override
    public void close() throws SQLException {
        close(selectStatementsAll);
        super.close();
    }
}