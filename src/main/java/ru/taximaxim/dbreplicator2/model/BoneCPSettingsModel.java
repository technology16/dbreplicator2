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
@Table(name = "bone_cp_settings")
public class BoneCPSettingsModel implements BoneCPSettings {
    /**
     * Минимальное количество соединений в пуле
     */
    static final int MIN_CONNECTIONS_PER_PARTITION = 1;
    /**
     * Максимальное количество соединений в пуле
     */
    static final int MAX_CONNECTIONS_PER_PARTITION = 100;
    /**
     * Количество партиций в пуле
     */
    static final int PARTITION_COUNT = 1;
    /**
     * Тайм аут на получение соединения из пула
     */
    static final long CONNECTION_TIMEOUT_IN_MS = 10000;
    /**
     * Тайм аут на закрытие соединения в пуле
     */
    static final long CLOSE_CONNECTION_WATCH_TIMEOUT_IN_MS = 0;

    /**
     * Конструктор по умолчанию
     */
    public BoneCPSettingsModel() {
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
     * @param poolId
     *            - имя пула
     * @param driver
     *            - наименование драйвера БД
     * @param url
     *            - строка подключения к БД
     * @param user
     *            - имя пользователя
     * @param pass
     *            - пароль
     * @param minConnectionsPerPartition
     *            - минимальное количество соединений
     * @param maxConnectionsPerPartition
     *            - максимальное количество соединений
     * @param partitionCount
     *            - количество порций соединений
     * @param connectionTimeoutInMs
     *            - таймаут получения соединения
     * @param closeConnectionWatchTimeoutInMs
     *            - таймаут закрытия соединения
     */
    public BoneCPSettingsModel(String poolId, String driver, String url, String user,
            String pass, int minConnectionsPerPartition, int maxConnectionsPerPartition,
            int partitionCount, long connectionTimeoutInMs,
            long closeConnectionWatchTimeoutInMs) {
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
     * @param poolId
     *            - имя пула
     * @param driver
     *            - наименование драйвера БД
     * @param url
     *            - строка подключения к БД
     * @param user
     *            - имя пользователя
     * @param pass
     *            - пароль
     */
    public BoneCPSettingsModel(String poolId, String driver, String url, String user,
            String pass) {
        this(poolId, driver, url, user, pass, MIN_CONNECTIONS_PER_PARTITION,
                MAX_CONNECTIONS_PER_PARTITION, PARTITION_COUNT, CONNECTION_TIMEOUT_IN_MS,
                CLOSE_CONNECTION_WATCH_TIMEOUT_IN_MS);
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
     * @param name
     *            the name to set
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
     * @param driver
     *            the driver to set
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
     * @param url
     *            the url to set
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
     * @param user
     *            the user to set
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
     * @param pass
     *            the pass to set
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
     * @param minConnectionsPerPartition
     *            the minConnectionsPerPartition to set
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
     * @param maxConnectionsPerPartition
     *            the maxConnectionsPerPartition to set
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
     * @param partitionCount
     *            the partitionCount to set
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
     * @param connectionTimeoutInMs
     *            the connectionTimeoutInMs to set
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
     * @param closeConnectionWatchTimeoutInMs
     *            the closeConnectionWatchTimeoutInMs to set
     */
    public void setCloseConnectionWatchTimeoutInMs(long closeConnectionWatchTimeoutInMs) {
        this.closeConnectionWatchTimeoutInMs = closeConnectionWatchTimeoutInMs;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (closeConnectionWatchTimeoutInMs ^ (closeConnectionWatchTimeoutInMs >>> 32));
        result = prime * result + (int) (connectionTimeoutInMs ^ (connectionTimeoutInMs >>> 32));
        result = prime * result + ((driver == null) ? 0 : driver.hashCode());
        result = prime * result + maxConnectionsPerPartition;
        result = prime * result + minConnectionsPerPartition;
        result = prime * result + partitionCount;
        result = prime * result + ((pass == null) ? 0 : pass.hashCode());
        result = prime * result + ((poolId == null) ? 0 : poolId.hashCode());
        result = prime * result + ((url == null) ? 0 : url.hashCode());
        result = prime * result + ((user == null) ? 0 : user.hashCode());
        return result;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof BoneCPSettingsModel)) {
            return false;
        }
        BoneCPSettingsModel other = (BoneCPSettingsModel) obj;
        if (closeConnectionWatchTimeoutInMs != other.closeConnectionWatchTimeoutInMs) {
            return false;
        }
        if (connectionTimeoutInMs != other.connectionTimeoutInMs) {
            return false;
        }
        if (driver == null) {
            if (other.driver != null) {
                return false;
            }
        } else if (!driver.equals(other.driver)) {
            return false;
        }
        if (maxConnectionsPerPartition != other.maxConnectionsPerPartition) {
            return false;
        }
        if (minConnectionsPerPartition != other.minConnectionsPerPartition) {
            return false;
        }
        if (partitionCount != other.partitionCount) {
            return false;
        }
        if (pass == null) {
            if (other.pass != null) {
                return false;
            }
        } else if (!pass.equals(other.pass)) {
            return false;
        }
        if (poolId == null) {
            if (other.poolId != null) {
                return false;
            }
        } else if (!poolId.equals(other.poolId)) {
            return false;
        }
        if (url == null) {
            if (other.url != null) {
                return false;
            }
        } else if (!url.equals(other.url)) {
            return false;
        }
        if (user == null) {
            if (other.user != null) {
                return false;
            }
        } else if (!user.equals(other.user)) {
            return false;
        }
        return true;
    }
}
