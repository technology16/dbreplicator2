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

import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;

import org.apache.log4j.Logger;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;


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
    public HikariCPSettingsModel(String poolId, String driver, String url, String user,
            String pass) {
        this.poolId = poolId;
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.pass = pass;
        this.maximumPoolSize = MAXIMUM_POOL_SIZE;
        this.initializationFailFast = INITIALIZATION_FAIL_FAST;
        this.connectionTimeout = CONNECTION_TIMEOUT;
        this.idleTimeout = IDLE_TIMEOUT;
        this.maxLifetime = MAX_LIFETIME;
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
     * @return the param
     */
    public String getParam() {
        return param;
    }

    /**
     * @param param
     *            the param to set
     */
    public void setParam(String param) {
        this.param = param;
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
    public Properties getProperties() {
        if(properties == null) {
            properties = new Properties();
            if(getParam() != null){
                try {
                    properties.load(new StringReader(getParam()));
                } catch (IOException e) {
                    Logger.getLogger(HikariCPSettingsModel.class).error("Ошибка при чтение параметров [" + getParam() + "]!", e);
                }
            }
        }
        return properties;
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
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
        if (!(obj instanceof HikariCPSettingsModel)) {
            return false;
        }
        HikariCPSettingsModel other = (HikariCPSettingsModel) obj;
        if (connectionTimeout != other.connectionTimeout) {
            return false;
        }
        if (driver == null) {
            if (other.driver != null) {
                return false;
            }
        } else if (!driver.equals(other.driver)) {
            return false;
        }
        if (idleTimeout != other.idleTimeout) {
            return false;
        }
        if (initializationFailFast != other.initializationFailFast) {
            return false;
        }
        if (maxLifetime != other.maxLifetime) {
            return false;
        }
        if (maximumPoolSize != other.maximumPoolSize) {
            return false;
        }
        if (param == null) {
            if (other.param != null) {
                return false;
            }
        } else if (!param.equals(other.param)) {
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
