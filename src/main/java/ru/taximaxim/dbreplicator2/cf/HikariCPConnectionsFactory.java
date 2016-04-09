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

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.model.HikariCPSettingsModel;
import ru.taximaxim.dbreplicator2.model.HikariCPSettingsService;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Фабрика соединений на основе пула соединений HikariCP
 *
 * @author petrov_im
 *
 */
public class HikariCPConnectionsFactory implements ConnectionFactory {

    private static final Logger LOG = Logger.getLogger(HikariCPConnectionsFactory.class);

    /**
     * Инициализированные именнованные пулы соединений
     */
    private final Map<String, HikariDataSource> connectionPools;

    /**
     * Хранилище настроек
     */
    private final HikariCPSettingsService settingStorage;

    /**
     * Конструктор фабрики
     * 
     * @param settingStorage ссылка на объект хранилища настроек
     */
    public HikariCPConnectionsFactory(HikariCPSettingsService settingStorage) {
        this.settingStorage = settingStorage;
        connectionPools = new HashMap<String, HikariDataSource>();
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.cf.ConnectionFactory#get(java.lang.String)
     */
    public DataSource get(String poolName) {
        HikariDataSource connectionPool;

        synchronized (connectionPools) {
            connectionPool = connectionPools.get(poolName);
            if (connectionPool == null) {
                HikariCPSettingsModel hikariCPSettings =
                        settingStorage.getDataBaseSettingsByName(poolName);

                if (hikariCPSettings == null) {
                    LOG.error("Не найден пул соединений с базой данных: " + poolName);
                    return null;
                }

                HikariConfig config = new HikariConfig();
                
                config.setPoolName(hikariCPSettings.getPoolId());
                config.setDriverClassName(hikariCPSettings.getDriver()); 
                config.setJdbcUrl(hikariCPSettings.getUrl());
                config.setUsername(hikariCPSettings.getUser());
                config.setPassword(hikariCPSettings.getPass());
                config.setInitializationFailFast(hikariCPSettings.getInitializationFailFast());
                config.setMaximumPoolSize(hikariCPSettings.getMaximumPoolSize());
                config.setConnectionTimeout(hikariCPSettings.getConnectionTimeout());
                config.setIdleTimeout(hikariCPSettings.getIdleTimeout());
                config.setMaxLifetime(hikariCPSettings.getMaxLifetime());
                config.setRegisterMbeans(true);
                
                connectionPool = new HikariDataSource(config);
                connectionPools.put(poolName, connectionPool);
            }
        }
        
        return connectionPool;
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.cf.ConnectionFactory#close(java.lang.String)
     */
    public void close(String poolName) {
        synchronized (connectionPools) {
            connectionPools.get(poolName).close();
            connectionPools.remove(poolName);
        }
    }

    /* (non-Javadoc)
     * @see ru.taximaxim.dbreplicator2.cf.ConnectionFactory#close()
     */
    public void close() {
        synchronized (connectionPools) {
            for (Map.Entry<String, HikariDataSource> entry : connectionPools.entrySet()) {
                entry.getValue().close();
            }
            connectionPools.clear();
        }
    }
}
