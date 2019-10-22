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

/**
 * Функции для работы с метаданными
 * 
 */
package ru.taximaxim.dbreplicator2.replica.strategies.replication.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

import ru.taximaxim.dbreplicator2.el.FatalReplicationException;
import ru.taximaxim.dbreplicator2.model.TableModel;

/**
 * Интерфейс для работы с реплицируемыми данными
 * 
 * @author volodin_aa
 *
 */
public interface DataService extends AutoCloseable {

    /**
     * Инициализация с кешированием подготовленного запроса для удаления лог
     * записи из источника
     * 
     * @return the deleteDestStatements
     * @throws SQLException
     * @throws FatalReplicationException 
     */
     PreparedStatement getDeleteStatement(TableModel table)
            throws FatalReplicationException;

    /**
     * Инициализация с кешированием подготовленного запроса для извлечения
     * записи из источника
     * 
     * @return the selectSourceStatements
     * @throws SQLException
     * @throws FatalReplicationException 
     */
    PreparedStatement getSelectStatement(TableModel table)
            throws FatalReplicationException;

    /**
     * Инициализация с кешированием подготовленного запроса обновления записи в
     * приемнике
     * 
     * @return the updateDestStatements
     * @throws SQLException
     * @throws FatalReplicationException 
     */
    PreparedStatement getUpdateStatement(TableModel table, Collection<String> avaliableCals)
            throws FatalReplicationException;

    /**
     * Инициализация с кешированием подготовленного запроса для вставки записи в
     * приемник
     * 
     * @return the insertDestStatements
     * @throws SQLException
     * @throws FatalReplicationException 
     */
    PreparedStatement getInsertStatement(TableModel table, Collection<String> avaliableCals)
            throws FatalReplicationException;
    
    /**
     * Кешированное получение списка ключевых колонок
     * 
     * @param table.getName()
     * @return
     * @throws SQLException
     * @throws FatalReplicationException 
     */
    Set<String> getPriCols(TableModel table) throws FatalReplicationException;

    /**
     * Кешированное получение списка всех колонок
     * 
     * @param table.getName()
     * @return
     * @throws SQLException
     * @throws FatalReplicationException 
     */
    Set<String> getAllCols(TableModel table) throws FatalReplicationException;

    /**
     * Кешированное получение списка всех доступных колонок
     * 
     * @param table.getName()
     * @return
     * @throws SQLException
     * @throws FatalReplicationException 
     */
    Set<String> getAllAvaliableCols(TableModel table, Collection<String> avaliableCals) throws FatalReplicationException;
    
    /**
     * Кешированное получение списка колонок с данными
     * 
     * @param table.getName()
     * @return
     * @throws SQLException
     * @throws FatalReplicationException 
     */
    Set<String> getDataCols(TableModel table)
            throws FatalReplicationException;    
    
    /**
     * Кешированное получение списка всех доступных колонок с данными
     * 
     * @param table.getName()
     * @return
     * @throws SQLException
     * @throws FatalReplicationException 
     */
    Set<String> getAvaliableDataCols(TableModel table, Collection<String> avaliableCals)
            throws FatalReplicationException;    
    
    /**
     * Кешированное получение списка автоинкрементных колонок
     * 
     * @param connection
     * @param table.getName()
     * @return
     * @throws SQLException
     */
    Set<String> getIdentityCols(TableModel table) throws FatalReplicationException;
    
    /**
     * Кешированное получение списка игнорируемых колонок
     * 
     * @param table.getName()
     * @return
     * @throws SQLException
     * @throws FatalReplicationException 
     */
    Set<String> getIgnoredCols(TableModel table) throws FatalReplicationException;

    /**
     * Кешированное получение списка реплицируеммых колонок
     * 
     * @param table.getName()
     * @return
     * @throws SQLException
     * @throws FatalReplicationException 
     */
    Set<String> getRequiredCols(TableModel table) throws FatalReplicationException;
    
    /**
     * Устанавливает сессионную переменную с именем текущео владельца записи.
     * Триггер который отслеживает изменение записей реагирует на этот флаг и заполняет
     * соответствующее поле в таблице rep2_superlog.
     * 
     * @throws Exception
     */
    void setRepServerName(String repServerName)
            throws SQLException;

    /**
     * Получение кешированного соединения
     * 
     * @return
     */
    Connection getConnection() throws FatalReplicationException;}