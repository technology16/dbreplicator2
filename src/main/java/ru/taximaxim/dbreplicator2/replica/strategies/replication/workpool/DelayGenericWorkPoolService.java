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

package ru.taximaxim.dbreplicator2.replica.strategies.replication.workpool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;

import ru.taximaxim.dbreplicator2.el.ErrorsLogService;


/**
 * @author volodin_aa
 *
 */
public class DelayGenericWorkPoolService extends GenericWorkPoolService implements WorkPoolService, AutoCloseable {

    private PreparedStatement lastOperationsStatement;
    private int period = 300000; 
    /**
     * Конструктор
     * @param connection
     * @param errorsLog
     */
    public DelayGenericWorkPoolService(Connection connection, ErrorsLogService errorsLog) {
        super(connection, errorsLog);
    }

    /**
     * Установка периода
     * @param period
     */
    public void setPeriod(int period) {
        this.period = period;
    }
    
    /**
     * Получение периода
     * @return
     */
    public int getPeriod() {
        return period;
    }
    
    @Override
    public PreparedStatement getLastOperationsStatement() throws SQLException {
        if (lastOperationsStatement == null) {
            lastOperationsStatement = 
                getConnection().prepareStatement("SELECT * FROM rep2_workpool_data WHERE id_runner=? AND id_superlog IN (SELECT MAX(id_superlog) AS id_superlog FROM rep2_workpool_data WHERE id_runner=? and c_date<=? GROUP BY id_foreign, id_table ORDER BY id_superlog LIMIT ? OFFSET ?) ORDER BY id_superlog ",
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY);
        }
        
        return lastOperationsStatement;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.WorkPoolService#getLastOperations(int, int, int)
     */
    @Override
    public ResultSet getLastOperations(int runnerId, int fetchSize, int offset)
            throws SQLException {
        PreparedStatement statement = getLastOperationsStatement();
        
        statement.setInt(1, runnerId);
        statement.setInt(2, runnerId);
        
        Timestamp date = new Timestamp(Calendar.getInstance().getTimeInMillis() - getPeriod());
        statement.setTimestamp(3, date);
        // Извлекаем частями равными fetchSize 
        statement.setFetchSize(fetchSize);
        
        // По задаче #2327
        // Задаем первоначальное смещение выборки равное 0.
        // При появлении ошибочных записей будем его увеличивать на 1.
        statement.setInt(4, fetchSize);
        statement.setInt(5, offset);
        
        return statement.executeQuery();
    }  
    
    @Override
    public void close() throws SQLException {
        close(lastOperationsStatement);
        super.close();
    }
}
