package ru.taximaxim.dbreplicator2.replica.strategies.replication.data;

import java.sql.Connection;

public class DataServiceSkeleton {

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

}