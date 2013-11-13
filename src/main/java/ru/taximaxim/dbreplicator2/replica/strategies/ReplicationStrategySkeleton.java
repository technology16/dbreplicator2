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
package ru.taximaxim.dbreplicator2.replica.strategies;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Заготовка стратегии репликации
 * 
 * @author volodin_aa
 *
 */
public class ReplicationStrategySkeleton {

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
    protected void trackError(String message, Connection connection,
            ResultSet operation) throws SQLException{
        // Увеличиваем счетчик ошибок на 1
        PreparedStatement incErrorsCount = 
                connection.prepareStatement("UPDATE rep2_workpool_data SET c_errors_count = COALESCE(c_errors_count, 0) + 1, c_last_error=?, c_last_error_date=? WHERE id_runner=? AND id_foreign=? AND id_table=? AND id_superlog<=?");
        incErrorsCount.setString(1, message);
        incErrorsCount.setTimestamp(2, new Timestamp(new Date().getTime()));
        incErrorsCount.setInt(3, operation.getInt("id_runner"));
        incErrorsCount.setLong(4, operation.getLong("id_foreign"));
        incErrorsCount.setString(5, operation.getString("id_table"));
        incErrorsCount.setLong(6, operation.getLong("id_superlog"));
        incErrorsCount.executeUpdate();
    }
    
}