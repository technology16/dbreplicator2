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
package ru.taximaxim.dbreplicator2.replica.strategies.replication;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import ru.taximaxim.dbreplicator2.model.IgnoreColumnsTableModel;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.model.TableModel;

/**
 * Заготовка стратегии репликации
 * 
 * @author volodin_aa
 *
 */
public class Skeleton {

    private Map<String, List<String>> ignoreCols = new TreeMap<String, List<String>>(String.CASE_INSENSITIVE_ORDER);
    
    /**
     * Функция удаления данных об операциях успешно реплицированной записи
     * 
     * @param deleteWorkPoolData
     * @param operationsResult
     * @throws SQLException
     */
    protected void clearWorkPoolData(PreparedStatement deleteWorkPoolData, 
            ResultSet operationsResult) throws SQLException {
        // Очищаем данные о текущей записи из набора данных реплики
        deleteWorkPoolData.setInt(1, operationsResult.getInt("id_runner"));
        deleteWorkPoolData.setLong(2, operationsResult.getLong("id_foreign"));
        deleteWorkPoolData.setString(3, operationsResult.getString("id_table"));
        deleteWorkPoolData.setLong(4, operationsResult.getLong("id_superlog"));
        deleteWorkPoolData.addBatch();
    }

    /**
     * Функция записи информации об ошибке
     * @throws SQLException 
     */
    protected void trackError(String message, Throwable e, Connection connection,
            ResultSet operation) throws SQLException{
        // Формируем сообщение об ошибке
        Writer writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        e.printStackTrace(printWriter);
        
        // Увеличиваем счетчик ошибок на 1
        PreparedStatement incErrorsCount = 
                connection.prepareStatement("UPDATE rep2_workpool_data SET c_errors_count = c_errors_count + 1, c_last_error=?, c_last_error_date=? WHERE id_runner=? AND id_table=? AND id_foreign=?");
        incErrorsCount.setString(1, message + "\n" + writer.toString());
        incErrorsCount.setTimestamp(2, new Timestamp(new Date().getTime()));
        incErrorsCount.setInt(3, operation.getInt("id_runner"));
        incErrorsCount.setString(4, operation.getString("id_table"));
        incErrorsCount.setLong(5, operation.getLong("id_foreign"));
        incErrorsCount.executeUpdate();
    }
    
    protected void setIgnoreCols(StrategyModel data) throws SQLException {
        List<String> cols;
        List<TableModel> tableList = data.getRunner().getSource().getTables();
        for (TableModel tableModel : tableList) {
            cols = new ArrayList<String>();
            for (IgnoreColumnsTableModel ignoreCols : tableModel.getIgnoreColumnsTable()) {
                cols.add(ignoreCols.getColumnName());
            }
            ignoreCols.put(tableModel.getName(), cols);
        }
    }
    
    protected List<String> getIgnoreCols(String tableName) {
        if(ignoreCols.get(tableName) == null) {
            return new ArrayList<String>();
        }
        else {
            return ignoreCols.get(tableName);
        }
    }
    
}