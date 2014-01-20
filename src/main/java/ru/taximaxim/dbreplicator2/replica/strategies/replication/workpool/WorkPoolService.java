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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * ИНтерфейс для инкапсуляции работы стратегии с данными в очереди репликации.
 * Необходим для реализации стратегии шаблонный метод.
 * 
 * @author volodin_aa
 *
 */
public interface WorkPoolService {
    /**
     * value = "id_table"
     */
    String ID_TABLE = "id_table";
    /**
     * value = "id_foreign"
     */
    String ID_FOREIGN = "id_foreign";
    /**
     * value = "id_superlog"
     */
    String ID_SUPERLOG = "id_superlog";
    /**
     * value = "id_runner"
     */
    String ID_RUNNER = "id_runner";
    
    /**
     * Получение подготовленного выражения для выборки последних операций
     * 
     * @return
     * @throws SQLException 
     */
    PreparedStatement getLastOperationsStatement() throws SQLException;
    
    /**
     * Получение подготовленного выражения для выборки последних операций
     * 
     * @return
     * @throws SQLException 
     */
    ResultSet getLastOperations(int runnerId, int fetchSize, int offset) throws SQLException;
    
    PreparedStatement getClearWorkPoolDataStatement() throws SQLException;
    
    void clearWorkPoolData(ResultSet operationsResult) throws SQLException;
    
    void trackError(String message, SQLException e, ResultSet operation) 
            throws SQLException;

}
