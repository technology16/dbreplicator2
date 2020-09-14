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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

import ru.taximaxim.dbreplicator2.utils.Utils;

/**
 * Персистентный класс настроек HikariCP
 *
 * @author petrov_im
 *
 */
@Entity
@Table(name = "hikari_cp_settings")
public class HikariCPSettingsModel implements Serializable {

    private static final long serialVersionUID = 2L;

    private static final String PROPERTY_IS_ENABLED = "is_enabled";
    private static final String PROPERTY_SHARD = "shard";

    private static final int MAXIMUM_POOL_SIZE = 3;
    private static final boolean INITIALIZATION_FAIL_FAST = false;
    private static final int CONNECTION_TIMEOUT = 30000;
    private static final int IDLE_TIMEOUT = 600000;
    private static final int MAX_LIFETIME = 600000;

    /**
     * Имя пула
     */
    @Id
    @Column(name = "id_pool")
    private String poolId;
    /**
     * Наименование драйвера БД
     */
    @Column(name = "driver")
    private String driver;
    /**
     * Строка подключения к БД
     */
    @Column(name = "url")
    private String url;
    /**
     * Имя пользователя
     */
    @Column(name = "user")
    private String user;
    /**
     * Пароль
     */
    @Column(name = "pass")
    private String pass;

    /**
     * Таймаут получения соединения (по-умолчанию 30 секунд)
     */
    @Column(name = "connection_timeout")
    private int connectionTimeout;

    /**
     * Время, в течении которого соединение может находиться в
     * соостоянии idle (по-умолчанию 10 минут)
     */
    @Column(name = "idle_timeout")
    private int idleTimeout;

    /**
     * Максимальное время жихни соедения в пуле
     * (по-умолчанию 30 минут)
     */
    @Column(name = "max_lifetime")
    private int maxLifetime;

    /**
     * Максимальное количество соединений
     */
    @Column(name = "max_pool_size")
    private int maximumPoolSize;

    @Column(name = "init_fail_fast")
    private boolean initializationFailFast;

    /**
     * Дополнительные параметры
     */
    @Column(name = "param", length = 20000)
    private String param;

    /**
     * Поле для получения настроек
     */
    private Properties properties;

    /**
     * Список обработчиков
     */
    @Column
    @OneToMany(targetEntity=RunnerModel.class, mappedBy="source", fetch=FetchType.EAGER)
    @Fetch(FetchMode.SELECT)
    private List<RunnerModel> runners;

    /**
     * Конструктор по умолчанию
     */
    public HikariCPSettingsModel() {
        // Для Hibernate
    }

    /**
     * Инициализация настроек HikariCP
     *
     * @param poolId имя пула
     * @param driver драйвер БД
     * @param url путь к БД
     * @param user пользователь
     * @param pass пароль
     * @param maximumPoolSize максимальное количество соединений
     * @param initializationFailFast проверка доступность БД при старте
     * @param connectionTimeout время ожидания свободного соединения, мс
     * @param idleTimeout время удержания в пуле простаивающего соединения, мс
     * @param maxLifetime время жизни коннекшена в базе, мс
     */
    public HikariCPSettingsModel(String poolId, String driver, String url, String user,
            String pass, int maximumPoolSize, boolean initializationFailFast,
            int connectionTimeout, int idleTimeout, int maxLifetime) {
        this.poolId = poolId;
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.pass = pass;
        this.maximumPoolSize = maximumPoolSize;
        this.initializationFailFast = initializationFailFast;
        this.connectionTimeout = connectionTimeout;
        this.idleTimeout = idleTimeout;
        this.maxLifetime = maxLifetime;
    }

    /**
     * Инициализация минимальных настроек HikariCP
     *
     * @param poolId имя пула
     * @param driver драйвер БД
     * @param url путь к БД
     * @param user пользователь
     * @param pass пароль
     */
    public HikariCPSettingsModel(String poolId, String driver, String url,
            String user, String pass) {
        this(poolId, driver, url, user, pass, MAXIMUM_POOL_SIZE,
                INITIALIZATION_FAIL_FAST, CONNECTION_TIMEOUT, IDLE_TIMEOUT, MAX_LIFETIME);
    }

    /**
     * Получение имени пула соединений
     *
     * @return the name
     */
    public String getPoolId() {
        return poolId;
    }

    /**
     * Установка имени пула
     *
     * @param name
     *            the name to set
     */
    public void setPoolId(String poolId) {
        this.poolId = poolId;
    }

    /**
     * Получение дравера БД
     *
     * @return the driver
     */
    public String getDriver() {
        return driver;
    }

    /**
     * Измение драйвера
     *
     * @param driver
     *            the driver to set
     */
    public void setDriver(String driver) {
        this.driver = driver;
    }

    /**
     * @return the url
     */
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
     * @return the maximumPoolSize
     */
    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    /**
     * @param maximumPoolSize
     *            the maximumPoolSize to set
     */
    public void setMaximumPoolSize(int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
    }

    /**
     * @return the initializationFailFast
     */
    public boolean getInitializationFailFast() {
        return initializationFailFast;
    }

    /**
     * @param initializationFailFast
     *            the initializationFailFast to set
     */
    public void setInitializationFailFast(boolean initializationFailFast) {
        this.initializationFailFast = initializationFailFast;
    }

    /**
     * @return the isEnabled
     */
    public boolean isEnabled() {
        return Boolean.parseBoolean(getProperties().getProperty(PROPERTY_IS_ENABLED, "true"));
    }

    /**
     * @param isEnabled
     *            the isEnabled to set
     */
    public void setEnabled(boolean isEnabled) {
        setParam(PROPERTY_IS_ENABLED, Boolean.toString(isEnabled));
    }

    /**
     * Получение параметра по ключу
     *
     * @param property
     *            имя параметра
     * @return значение параметра или null
     */
    public String getParam(String property) {
        return getProperties().getProperty(property);
    }

    /**
     * Установка параметра подключения
     *
     * @param key параметр
     * @param value значение
     */
    public void setParam(String key, String value) {
        getProperties().put(key, value);
        param = Utils.convertPropertiesToParamString(getProperties());
    }

    /**
     * @return имя шарда из списка параметров
     */
    public String getShard() {
        return getParam(PROPERTY_SHARD);
    }

    /**
     * @param shard
     *            вносит имя шарда в список параметров
     */
    public void setShard(String shard) {
        setParam(PROPERTY_SHARD, shard);
    }

    /**
     * @return the idleTimeout
     */
    public int getIdleTimeout() {
        return idleTimeout;
    }

    /**
     * @param idleTimeout
     *            the idleTimeout to set
     */
    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    /**
     * @return the maxLifetime
     */
    public int getMaxLifetime() {
        return maxLifetime;
    }

    /**
     * @param maxLifetime
     *            the maxLifetime to set
     */
    public void setMaxLifetime(int maxLifetime) {
        this.maxLifetime = maxLifetime;
    }

    /**
     * Получение списка раннеров
     */
    public List<RunnerModel> getRunners() {
        if (runners == null) {
            runners = new ArrayList<>();
        }

        return runners;
    }

    /**
     * Получение конкретного раннера пула
     *
     * @param runnerId идентификатор раннера
     * @return
     */
    public RunnerModel getRunner(int runnerId) {
        for (RunnerModel runner : getRunners()) {
            if (runner.getId() == runnerId) {
                return runner;
            }
        }
        return null;
    }

    /**
     * Сохранение списка обработчиков
     */
    public void setRunners(List<RunnerModel> runners) {
        this.runners = runners;
    }

    /**
     * @return the connectionTimeout
     */
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    /**
     * @param connectionTimeout
     *            the connectionTimeout to set
     */
    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    /**
     * Получение дополнительных настроек
     *
     * @return
     */
    private Properties getProperties() {
        if (properties == null) {
            properties = Utils.convertParamStringToProperties(param);
        }

        return properties;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + connectionTimeout;
        result = prime * result + ((driver == null) ? 0 : driver.hashCode());
        result = prime * result + idleTimeout;
        result = prime * result + (initializationFailFast ? 1231 : 1237);
        result = prime * result + maxLifetime;
        result = prime * result + maximumPoolSize;
        result = prime * result + ((param == null) ? 0 : param.hashCode());
        result = prime * result + ((pass == null) ? 0 : pass.hashCode());
        result = prime * result + ((poolId == null) ? 0 : poolId.hashCode());
        result = prime * result + ((url == null) ? 0 : url.hashCode());
        result = prime * result + ((user == null) ? 0 : user.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (!(obj instanceof HikariCPSettingsModel)) {
            return false;
        }

        HikariCPSettingsModel other = (HikariCPSettingsModel) obj;
        return connectionTimeout == other.connectionTimeout
                && Objects.equals(driver, other.driver)
                && idleTimeout == other.idleTimeout
                && initializationFailFast == other.initializationFailFast
                && maxLifetime == other.maxLifetime
                && maximumPoolSize == other.maximumPoolSize
                && Objects.equals(param, other.param)
                && Objects.equals(pass, other.pass)
                && Objects.equals(poolId, other.poolId)
                && Objects.equals(url, other.url)
                && Objects.equals(user, other.user);
    }
}
