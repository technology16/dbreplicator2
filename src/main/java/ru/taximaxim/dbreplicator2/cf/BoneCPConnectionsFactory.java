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

package ru.taximaxim.dbreplicator2.cf;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.model.BoneCPDataBaseSettingsStorage;
import ru.taximaxim.dbreplicator2.model.BoneCPSettings;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

/**
 * Фабрика соединений на основе пула соединений BoneCP
 *
 * @author volodin_aa
 *
 */
public class BoneCPConnectionsFactory implements ConnectionFactory {

    private static final Logger LOG = Logger.getLogger(BoneCPConnectionsFactory.class);

    /**
     * Инициализированные именнованные пулы соединений
     */
    private Map<String, BoneCP> connectionPools;

    /**
     * Хранилище настроек
     */
    private BoneCPDataBaseSettingsStorage settingStorage;

    /**
     * Конструктор фабрики
     *
     * @param entityManager
     *            - ссылка на объект хранилища настроек
     */
    public BoneCPConnectionsFactory(BoneCPDataBaseSettingsStorage settingStorage) {
        this.settingStorage = settingStorage;
        connectionPools = new HashMap<String, BoneCP>();
    }

    /*
     * (non-Javadoc)
     *
     * @see ru.taximaxim.dbreplicator2.ConnectionsFactory.IConnectionsFactory#
     * getConnection(java.lang.String)
     */
    public Connection getConnection(String poolName) throws SQLException,
            ClassNotFoundException {
        BoneCP connectionPool;

        synchronized (connectionPools) {
            connectionPool = connectionPools.get(poolName);
            if (connectionPool == null) {
                BoneCPSettings boneCPSettings =
                        settingStorage.getDataBaseSettingsByName(poolName);

                if (boneCPSettings == null) {
                    LOG.error("Не найден пул соединений с базой данных: " + poolName);
                    return null;
                }

                Class.forName(boneCPSettings.getDriver());

                BoneCPConfig config = new BoneCPConfig();
                config.setJdbcUrl(boneCPSettings.getUrl());
                config.setUsername(boneCPSettings.getUser());
                config.setPassword(boneCPSettings.getPass());

                config.setMinConnectionsPerPartition(boneCPSettings
                        .getMinConnectionsPerPartition());
                config.setMaxConnectionsPerPartition(boneCPSettings
                        .getMaxConnectionsPerPartition());
                config.setPartitionCount(boneCPSettings.getPartitionCount());
                config.setConnectionTimeoutInMs(boneCPSettings
                        .getConnectionTimeoutInMs());
                config.setCloseConnectionWatchTimeoutInMs(boneCPSettings
                        .getCloseConnectionWatchTimeoutInMs());

                connectionPool = new BoneCP(config);

                connectionPools.put(poolName, connectionPool);
            }
        }
        return connectionPool.getConnection();
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * ru.taximaxim.dbreplicator2.ConnectionsFactory.IConnectionsFactory#close
     * (java.lang.String)
     */
    public void close(String poolName) {
        synchronized (connectionPools) {
            connectionPools.get(poolName).close();
            connectionPools.remove(poolName);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * ru.taximaxim.dbreplicator2.ConnectionsFactory.IConnectionsFactory#close()
     */
    public void close() {
        synchronized (connectionPools) {
            for (String poolName : connectionPools.keySet()) {
                connectionPools.get(poolName).close();
            }
            connectionPools.clear();
        }
    }
}
