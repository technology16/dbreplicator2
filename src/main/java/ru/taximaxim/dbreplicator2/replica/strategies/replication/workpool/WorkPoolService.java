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
     * values = "id_table"
     */
    String ID_TABLE = "id_table";
    /**
     * values = "c_operation"
     */
    String C_OPERATION = "c_operation";
    /**
     * values = "id_foreign"
     */
    String ID_FOREIGN = "id_foreign";
    /**
     * values = "id_superlog"
     */
    String ID_SUPERLOG = "id_superlog";
    /**
     * values = "id_superlog" максимальный id_superlog из выборки
     */
    String ID_SUPERLOG_MAX = "id_superlog_max";
    /**
     * values = "id_superlog" мимнимальный id_superlog из выборки
     */
    String ID_SUPERLOG_MIN = "id_superlog_min";
    /**
     * values = "id_pool"
     */
    String ID_POOL = "id_pool";
    /**
     * values = "c_date"
     */
    String C_DATE = "c_date";
    /**
     * values = "id_transaction"
     */
    String ID_TRANSACTION = "id_transaction";

    /**
     * value = "id_runner"
     */
    String ID_RUNNER = "id_runner";

    /**
     * value = "records_count"
     */
    String RECORDS_COUNT = "records_count";
    
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
     * 
     * @throws SQLException 
     */
    ResultSet getLastOperations(int runnerId, int fetchSize, int offset) throws SQLException;
    
    /**
     * Получение подготовленного запроса для удаления обработанных данных из рабочего набора
     * 
     * @return подготовленный запрос для удаления обработанных данных из рабочего набора
     * 
     * @throws SQLException
     */
    PreparedStatement getClearWorkPoolDataStatement() throws SQLException;
    
    /**
     * Функция удаления обработанных данных из рабочего набора
     * 
     * @param operationsResult
     * 
     * @throws SQLException
     */
    void clearWorkPoolData(ResultSet operationsResult) throws SQLException;
    
    /**
     * Функция записи информации об ошибке в рабочий набор
     * 
     * @param message  - сообщение
     * @param e - ошибка
     * @param operation - тип операции
     * 
     * @throws SQLException
     */
    void trackError(String message, SQLException e, ResultSet operation) throws SQLException;
    
    /**
     * Получение имени текущей таблицы
     * 
     * @param resultSet - текущая запись в рабочем наборе
     * 
     * @return - имя таблицы
     * 
     * @throws SQLException
     */
    String getTable(ResultSet resultSet) throws SQLException;
    
    /**
     * Получение идентификатора текущей записи
     * 
     * @param resultSet
     * @return
     * @throws SQLException
     */
    Long getForeign(ResultSet resultSet) throws SQLException;
    
    /**
     * Получение идентификатора текущего раннера
     * 
     * @param resultSet
     * @return
     * @throws SQLException
     */
    int getRunner(ResultSet resultSet) throws SQLException;
    
    /**
     * Получение максимального идентификатора текущей операции
     * 
     * @param resultSet
     * @return
     * @throws SQLException
     */
    Long getSuperlog(ResultSet resultSet) throws SQLException;
    
    /**
     * Получение максимального идентификатора текущей операции
     * 
     * @param resultSet
     * @return
     * @throws SQLException
     */
    Long getSuperlogMax(ResultSet resultSet) throws SQLException;
    
    /**
     * Получение минимального идентификатора текущей операции
     * 
     * @param resultSet
     * @return
     * @throws SQLException
     */
    Long getSuperlogMin(ResultSet resultSet) throws SQLException;
    
    /**
     * Получение колличества сгруппированных записей
     * 
     * @param resultSet
     * @return
     * @throws SQLException
     */
    int getRecordsCount(ResultSet resultSet) throws SQLException;
}
