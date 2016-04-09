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

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.jdbc.StatementsHashMap;
import ru.taximaxim.dbreplicator2.model.TableModel;

/**
 * @author volodin_aa
 * 
 */
public class GenericDataTypeService extends GenericDataService implements DataService {
    /**
     * Кешированные запросы получения данных из источника и приемника
     */
    private final StatementsHashMap<TableModel, PreparedStatement> selectStatementsAll = new StatementsHashMap<TableModel, PreparedStatement>();
    private final Map<TableModel, Map<String, Integer>> allColsTypes = new HashMap<TableModel, Map<String, Integer>>();

    /**
     * Конструктор на основе подключения к БД
     */
    public GenericDataTypeService(DataSource dataSource) {
        super(dataSource);
    }

    /**
     * Получение кеша запросов на выборку данных из источника и приемника
     * 
     * @return the selectStatements
     */
    protected Map<TableModel, PreparedStatement> getSelectStatementsAll() {
        return selectStatementsAll;
    }

    /**
     * Кешированное получение списка типов всех колонок
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
            for (String ignoredCol : getIgnoredCols(table)) {
                colsTypes.remove(ignoredCol);
            }

            // Оставляем обязательно реплицируемые колонки
            Set<String> requiredColsSet = getRequiredCols(table);
            if (requiredColsSet.size() != 0) {
                for (String colName : colsTypes.keySet()) {
                    if (!requiredColsSet.contains(colName)) {
                        colsTypes.remove(colName);
                    }
                }
            }

            allColsTypes.put(table, colsTypes);
        }
        return colsTypes;
    }

    @Override
    public void close() throws SQLException {
        try (StatementsHashMap<TableModel, PreparedStatement>  selectStatementsAll = this.selectStatementsAll) {
            super.close();
        }
    }
}