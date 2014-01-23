package ru.taximaxim.dbreplicator2.replica.strategies.replication.data;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.apache.log4j.Logger;

public class DataServiceSkeleton implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(DataServiceSkeleton.class);
    private Connection connection;

    public DataServiceSkeleton(Connection connection) {
        this.connection = connection;
    }

    /**
     * @return the connection
     */
    protected Connection getConnection() {
        return connection;
    }


    @Override
    public void close() throws SQLException {
        
    }
    
    /**
     * Закрыть Map<?, PreparedStatement>
     * @param statement
     * @throws SQLException
     */
    public void close(Map<?, PreparedStatement> sqlStatements) {
        for (PreparedStatement statement : sqlStatements.values()) {
            if (statement != null) {
                close(statement);
            }
        }
        sqlStatements.clear();
    }
    
    /**
     * Закрыть PreparedStatement
     * @param statement
     * @throws SQLException
     */
    public void close(PreparedStatement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.warn("Ошибка при попытке закрыть 'statement.close()': ", e);
            }
        }
    } 
}