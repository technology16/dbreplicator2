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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;


/**
 * @author volodin_aa
 *
 */
public class GenericWorkPoolService implements WorkPoolService {

    private Connection connection;
    
    /**
     * Конструктор на основе соединения к БД 
     */
    public GenericWorkPoolService(Connection connection) {
        this.connection = connection;
    }
    
    /**
     * @return the connection
     */
    protected Connection getConnection() {
        return connection;
    }

    private PreparedStatement clearWorkPoolDataStatement;

    private PreparedStatement lastOperationsStatement;

    @Override
    public PreparedStatement getLastOperationsStatement() throws SQLException {
        if (lastOperationsStatement == null) {
            lastOperationsStatement = 
                getConnection().prepareStatement("SELECT * FROM rep2_workpool_data WHERE id_superlog IN (SELECT MAX(id_superlog) AS id_superlog FROM rep2_workpool_data WHERE id_runner=? GROUP BY id_foreign, id_table ORDER BY id_superlog) ORDER BY id_superlog", 
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
        // Извлекаем частями равными fetchSize 
        statement.setFetchSize(fetchSize);
        
        return statement.executeQuery();
    }

    @Override
    public PreparedStatement getClearWorkPoolDataStatement() throws SQLException {
        if (clearWorkPoolDataStatement == null) {
            clearWorkPoolDataStatement = 
                    getConnection().prepareStatement("DELETE FROM rep2_workpool_data WHERE id_runner=? AND id_foreign=? AND id_table=? AND id_superlog<=?");
        }
        
        return clearWorkPoolDataStatement;
    }
    
    /**
     * Функция удаления данных об операциях успешно реплицированной записи
     * 
     * @param deleteWorkPoolData
     * @param operationsResult
     * @throws SQLException
     */
    public void clearWorkPoolData(ResultSet operationsResult) throws SQLException {
        // Очищаем данные о текущей записи из набора данных реплики
        PreparedStatement deleteWorkPoolData = 
                getClearWorkPoolDataStatement();
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
    public void trackError(String message, SQLException e, ResultSet operation) 
            throws SQLException{
        // Формируем сообщение об ошибке
        Writer writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        e.printStackTrace(printWriter);
        
        SQLException nextEx = e.getNextException();
        while (nextEx!=null){
            printWriter.println("Подробности: ");
            nextEx.printStackTrace(printWriter);
            nextEx = nextEx.getNextException();
        }

        // Увеличиваем счетчик ошибок на 1
        PreparedStatement incErrorsCount = 
                getConnection().prepareStatement("UPDATE rep2_workpool_data SET c_errors_count = c_errors_count + 1, c_last_error=?, c_last_error_date=? WHERE id_runner=? AND id_table=? AND id_foreign=?");
        incErrorsCount.setString(1, message + "\n" + writer.toString());
        incErrorsCount.setTimestamp(2, new Timestamp(new Date().getTime()));
        incErrorsCount.setInt(3, operation.getInt("id_runner"));
        incErrorsCount.setString(4, operation.getString("id_table"));
        incErrorsCount.setLong(5, operation.getLong("id_foreign"));
        incErrorsCount.executeUpdate();
    }
}
