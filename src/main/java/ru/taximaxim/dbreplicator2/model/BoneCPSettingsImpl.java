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
package ru.taximaxim.dbreplicator2.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

import ru.taximaxim.dbreplicator2.cf.BoneCPSettings;

/**
 * Персистентный класс настроек BoneCP
 * 
 * @author volodin_aa
 *
 */
@Entity
@Table( name = "bone_cp_settings" )
public class BoneCPSettingsImpl implements BoneCPSettings {
    /**
     * Минимальное количество соединений в пуле
     */
    final static int MIN_CONNECTIONS_PER_PARTITION = 1;
    /**
     * Максимальное количество соединений в пуле
     */
    final static int MAX_CONNECTIONS_PER_PARTITION = 100;
    /**
     * Количество партиций в пуле
     */
    final static int PARTITION_COUNT = 1;
    /**
     * Тайм аут на получение соединения из пула
     */
    final static long CONNECTION_TIMEOUT_IN_MS = 10000;
    /**
     * Тайм аут на закрытие соединения в пуле
     */
    final static long CLOSE_CONNECTION_WATCH_TIMEOUT_IN_MS = 0;
    
    /**
     * Конструктор по умолчанию
     */
    public BoneCPSettingsImpl() {
    }

    /**
     * Имя пула
     */
    private String poolId;
    /**
     * Наименование драйвера БД
     */
    private String driver;
    /**
     * Строка подключения к БД
     */
    private String url;
    /**
     * Имя пользователя
     */
    private String user;
    /**
     * Пароль
     */
    private String pass;
    
    /**
     * Минимальное количество соединений
     */
    private int minConnectionsPerPartition;
    /**
     * Максимальное количество соединений
     */
    private int maxConnectionsPerPartition;
    /**
     * Количество порций соединений
     */
    private int partitionCount;
    /**
     * Таймаут получения соединения
     */
    private long connectionTimeoutInMs;
    /**
     * Таймаут закрытия соединения
     */
    private long closeConnectionWatchTimeoutInMs;

    /**
     * Полный конструктор
     * 
     * @param poolId - имя пула
     * @param driver - наименование драйвера БД
     * @param url - строка подключения к БД
     * @param user - имя пользователя
     * @param pass - пароль
     * @param minConnectionsPerPartition - минимальное количество соединений
     * @param maxConnectionsPerPartition - максимальное количество соединений
     * @param partitionCount - количество порций соединений
     * @param connectionTimeoutInMs - таймаут получения соединения
     * @param closeConnectionWatchTimeoutInMs - таймаут закрытия соединения
     */
    public BoneCPSettingsImpl(String poolId, String driver, String url, String user, String pass,
            int minConnectionsPerPartition, int maxConnectionsPerPartition, int partitionCount,
            long connectionTimeoutInMs, long closeConnectionWatchTimeoutInMs) {
        this.poolId = poolId;
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.pass = pass;
        this.minConnectionsPerPartition = minConnectionsPerPartition;
        this.maxConnectionsPerPartition = maxConnectionsPerPartition;
        this.partitionCount = partitionCount;
        this.connectionTimeoutInMs = connectionTimeoutInMs;
        this.closeConnectionWatchTimeoutInMs = closeConnectionWatchTimeoutInMs;
    }
    
    /**
     * Сокращенный конструктор
     * 
     * @param poolId - имя пула
     * @param driver - наименование драйвера БД
     * @param url - строка подключения к БД
     * @param user - имя пользователя
     * @param pass - пароль
     */
    public BoneCPSettingsImpl(String poolId, String driver, String url, String user, String pass) {
        this(poolId, driver, url, user, pass, MIN_CONNECTIONS_PER_PARTITION, MAX_CONNECTIONS_PER_PARTITION, 
                PARTITION_COUNT, CONNECTION_TIMEOUT_IN_MS, CLOSE_CONNECTION_WATCH_TIMEOUT_IN_MS);
    }

    /**
     * @return the name
     */
    @Id
    @Column(name = "pool_id")
    public String getPoolId() {
        return poolId;
    }

    /**
     * @param name the name to set
     */
    public void setPoolId(String poolId) {
        this.poolId = poolId;
    }

    /**
     * @return the driver
     */
    @Column(name = "driver")
    public String getDriver() {
        return driver;
    }

    /**
     * @param driver the driver to set
     */
    public void setDriver(String driver) {
        this.driver = driver;
    }

    /**
     * @return the url
     */
    @Column(name = "url")
    public String getUrl() {
        return url;
    }

    /**
     * @param url the url to set
     */
    public void setUrl(String url) {
        this.url = url;
    }

    /**
     * @return the user
     */
    @Column(name = "user")
    public String getUser() {
        return user;
    }

    /**
     * @param user the user to set
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * @return the pass
     */
    @Column(name = "pass")
    public String getPass() {
        return pass;
    }

    /**
     * @param pass the pass to set
     */
    public void setPass(String pass) {
        this.pass = pass;
    }

    /**
     * @return the minConnectionsPerPartition
     */
    @Column(name = "min_connections_per_partition")
    public int getMinConnectionsPerPartition() {
        return minConnectionsPerPartition;
    }

    /**
     * @param minConnectionsPerPartition the minConnectionsPerPartition to set
     */
    public void setMinConnectionsPerPartition(int minConnectionsPerPartition) {
        this.minConnectionsPerPartition = minConnectionsPerPartition;
    }

    /**
     * @return the maxConnectionsPerPartition
     */
    @Column(name = "max_connections_per_partition")
    public int getMaxConnectionsPerPartition() {
        return maxConnectionsPerPartition;
    }

    /**
     * @param maxConnectionsPerPartition the maxConnectionsPerPartition to set
     */
    public void setMaxConnectionsPerPartition(int maxConnectionsPerPartition) {
        this.maxConnectionsPerPartition = maxConnectionsPerPartition;
    }

    /**
     * @return the partitionCount
     */
    @Column(name = "partition_count")
    public int getPartitionCount() {
        return partitionCount;
    }

    /**
     * @param partitionCount the partitionCount to set
     */
    public void setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
    }

    /**
     * @return the connectionTimeoutInMs
     */
    @Column(name = "connection_timeout_in_ms")
    public long getConnectionTimeoutInMs() {
        return connectionTimeoutInMs;
    }

    /**
     * @param connectionTimeoutInMs the connectionTimeoutInMs to set
     */
    public void setConnectionTimeoutInMs(long connectionTimeoutInMs) {
        this.connectionTimeoutInMs = connectionTimeoutInMs;
    }

    /**
     * @return the closeConnectionWatchTimeoutInMs
     */
    @Column(name = "close_connection_watch_timeout_in_ms")
    public long getCloseConnectionWatchTimeoutInMs() {
        return closeConnectionWatchTimeoutInMs;
    }

    /**
     * @param closeConnectionWatchTimeoutInMs the closeConnectionWatchTimeoutInMs to set
     */
    public void setCloseConnectionWatchTimeoutInMs(long closeConnectionWatchTimeoutInMs) {
        this.closeConnectionWatchTimeoutInMs = closeConnectionWatchTimeoutInMs;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BoneCPSettings) {
            return poolId.equals(((BoneCPSettings) obj).getPoolId()) &&
                    driver.equals(((BoneCPSettings) obj).getDriver()) &&
                    url.equals(((BoneCPSettings) obj).getUrl()) &&
                    user.equals(((BoneCPSettings) obj).getUser()) &&
                    pass.equals(((BoneCPSettings) obj).getPass()) &&
                    this.minConnectionsPerPartition == ((BoneCPSettings) obj).getMinConnectionsPerPartition() &&
                    this.maxConnectionsPerPartition == ((BoneCPSettings) obj).getMaxConnectionsPerPartition() &&
                    this.partitionCount == ((BoneCPSettings) obj).getPartitionCount() &&
                    this.connectionTimeoutInMs == ((BoneCPSettings) obj).getConnectionTimeoutInMs() &&
                    this.closeConnectionWatchTimeoutInMs == ((BoneCPSettings) obj).getCloseConnectionWatchTimeoutInMs();
        }
        return false;
    }


}
