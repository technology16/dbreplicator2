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

package ru.taximaxim.dbreplicator2.jdbc;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

/**
 * HashMap<K, V> содержащий V extends Statement и поддерживающий AutoCloseable
 * 
 * @author volodin_aa
 * @param <K>
 * @param <V>
 *
 */
public class StatementsHashMap<K, V extends Statement> extends HashMap<K, V> implements AutoCloseable {

    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Добавление эксепшена e к ex
     * 
     * @param ex
     * @param e
     */
    protected SQLException addException(SQLException ex, SQLException e) {
        if (ex == null) {
            return e;
        }

        ex.addSuppressed(e);
        return ex;
    }

    /* (non-Javadoc)
     * @see java.lang.AutoCloseable#close()
     */
    @Override
    public void close() throws SQLException {
        SQLException ex = null;
        for (Statement statement : values()) {
            // Пытаемся закрыть каждый 
            try {
                if (statement != null && !statement.isClosed()) {
                    statement.close();
                }
            } catch (SQLException e) {
                ex = addException(ex, e);
            }
        }
        this.clear();

        // При необходимости выплевываем ошибку
        if (ex != null) {
            throw ex;
        }
    }

}
