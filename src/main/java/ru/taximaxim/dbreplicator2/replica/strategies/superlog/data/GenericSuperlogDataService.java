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

package ru.taximaxim.dbreplicator2.replica.strategies.superlog.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * Реализация дата сервиса для обработки суперлога postgresql (H2)
 * 
 * @author petrov_im
 *
 */
public class GenericSuperlogDataService implements SuperlogDataService {

    protected PreparedStatement selectSuperlogStatement;
    protected PreparedStatement deleteSuperlogStatement;
    protected PreparedStatement insertWorkpoolStatement;
    protected PreparedStatement selectInitSuperlogStatement;

    protected Connection selectConnection;
    protected Connection deleteConnection;
    protected Connection targetConnection;

    protected final DataSource source;
    protected final DataSource target;
    protected final int fetchSize;

    /**
     * Инициализация дата сервиса
     * 
     * @param source пул источника
     * @param target пул приемника
     * @param fetchSize размер выборки
     */
    public GenericSuperlogDataService(DataSource source, DataSource target,
            int fetchSize) {
        this.source = source;
        this.target = target;
        this.fetchSize = fetchSize;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.strategies.superlog.data.SuperlogDataService#getFetchSize()
     */
    @Override
    public int getFetchSize() {
        return fetchSize;
    }

    protected Connection getSelectConnection() throws SQLException {
        if (selectConnection == null) {
            selectConnection = source.getConnection();
            selectConnection.setAutoCommit(true);
            selectConnection
                    .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
        return selectConnection;
    }

    protected Connection getDeleteConnection() throws SQLException {
        if (deleteConnection == null) {
            deleteConnection = source.getConnection();
            deleteConnection.setAutoCommit(false);
            deleteConnection
                    .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
        return deleteConnection;
    }

    protected Connection getTargetConnection() throws SQLException {
        if (targetConnection == null) {
            targetConnection = target.getConnection();
            targetConnection.setAutoCommit(false);
            targetConnection
                    .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        }
        return targetConnection;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.strategies.superlog.data.SuperlogDataService#getInitSelectSuperlogStatement()
     */
    @Override
    public PreparedStatement getInitSelectSuperlogStatement() throws SQLException {
        if (selectInitSuperlogStatement == null) {
            selectInitSuperlogStatement = getSelectConnection().prepareStatement(
                    String.format(
                            "SELECT * FROM rep2_superlog ORDER BY id_superlog limit %s",
                            fetchSize),
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        }

        return selectInitSuperlogStatement;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.strategies.superlog.data.SuperlogDataService#getSelectSuperlogStatement()
     */
    @Override
    public PreparedStatement getSelectSuperlogStatement() throws SQLException {
        if (selectSuperlogStatement == null) {
            selectSuperlogStatement = getSelectConnection().prepareStatement(
                    String.format(
                            "SELECT * FROM rep2_superlog where id_superlog > ? ORDER BY id_superlog limit %s",
                            fetchSize),
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        }

        return selectSuperlogStatement;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.strategies.superlog.data.SuperlogDataService#getDeleteSuperlogStatement()
     */
    @Override
    public PreparedStatement getDeleteSuperlogStatement() throws SQLException {
        if (deleteSuperlogStatement == null) {
            deleteSuperlogStatement = getDeleteConnection()
                    .prepareStatement("DELETE FROM rep2_superlog WHERE id_superlog=?");
        }

        return deleteSuperlogStatement;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.replica.strategies.superlog.data.SuperlogDataService#getInsertWorkpoolStatement()
     */
    @Override
    public PreparedStatement getInsertWorkpoolStatement() throws SQLException {
        if (insertWorkpoolStatement == null) {
            insertWorkpoolStatement = getTargetConnection().prepareStatement(
                    "INSERT INTO rep2_workpool_data (id_runner, id_superlog, id_foreign, "
                            + "id_table, c_operation, c_date, id_transaction) VALUES (?, ?, ?, ?, ?, ?, ?)");
        }

        return insertWorkpoolStatement;
    }

    /* (non-Javadoc)
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close() throws SQLException {
        try (Connection thisSelectConnection = this.selectConnection;
                Connection thisDeleteConnection = this.deleteConnection;
                Connection thisTargetConnection = this.targetConnection;
                PreparedStatement thisInsertWorkpoolStatement = this.insertWorkpoolStatement;
                PreparedStatement thisDeleteSuperlogStatement = this.deleteSuperlogStatement;
                PreparedStatement thisSelectSuperlogStatement = this.selectSuperlogStatement;
                PreparedStatement thisSelectInitSuperlogStatement = this.selectInitSuperlogStatement;) {
            // Автоматическое закрытие ресурсов
        }
    }
}
