/**
 * 
 */
package ru.taximaxim.dbreplicator2.utils;

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
