/* The MIT License (MIT)
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

package ru.taximaxim.dbreplicator2.utils;

import java.io.File;

import org.apache.log4j.Logger;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.cf.HikariCPConnectionsFactory;
import ru.taximaxim.dbreplicator2.cron.CronPool;
import ru.taximaxim.dbreplicator2.model.ApplicatonSettingsService;
import ru.taximaxim.dbreplicator2.model.CronSettingsService;
import ru.taximaxim.dbreplicator2.model.HikariCPSettingsService;
import ru.taximaxim.dbreplicator2.model.TaskSettingsService;
import ru.taximaxim.dbreplicator2.el.ErrorsLog;
import ru.taximaxim.dbreplicator2.stats.StatsService;
import ru.taximaxim.dbreplicator2.tasks.TasksPool;
import ru.taximaxim.dbreplicator2.tp.ThreadPool;


/**
 * @author volodin_aa
 *
 */
public final class Core {
    
    private static final Logger LOG = Logger.getLogger(Core.class);

    private static SessionFactory sessionFactory;

    private static ConnectionFactory connectionFactory;
    
    private static CronSettingsService cronSettingsService;
    
    private static CronPool cronPool;
    
    private static TaskSettingsService taskSettingsService;
    
    private static TasksPool tasksPool;
    
    private static Configuration configuration;
    
    private static ThreadPool threadPool;
    
    private static StatsService statsService;

    /**
     * Данный класс нельзя инстанциировать.
     */
    private Core() {
    }
    
    /**
     * Получение настроек из файла
     * 
     * @param hibernateXmlFile - путь к файлу настроек
     * @return
     */
    public static synchronized Configuration getConfiguration(String hibernateXmlFile){
        if (configuration == null) {
            configuration = new Configuration();
            if (hibernateXmlFile != null) {
                File conf = new File(hibernateXmlFile);
                configuration.configure(conf);
            } else {
                configuration.configure();
            }
        }
        return configuration;
    }

    /**
     * Получение настроек по умолчанию
     * 
     * @return
     */
    public static Configuration getConfiguration(){
        return getConfiguration((String) null);
    }
    
    /**
     * Обнуление конфигурации
     */
    public static void configurationClose(){
        configuration = null;
    }
    
    /**
     * Возвращает фабрику сессий гибернейта.
     * 
     * @param configuration - инициализированная конфигурация
     * @return фабрику сессий гибернейта.
     */
    public static synchronized SessionFactory getSessionFactory(Configuration configuration) {
        LOG.debug("Запрошено создание новой фабрики сессий hibernate");

        if (sessionFactory == null) {
            ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder()
                    .applySettings(configuration.getProperties())
                    .build();
            sessionFactory = configuration.buildSessionFactory(serviceRegistry);

            LOG.info("Создана новая фабрика сессий hibernate");
        }
        return sessionFactory;
    }
    
    /**
     * Возвращает фабрику сессий гибернейта.
     * 
     * @return фабрику сессий гибернейта.
     */
    public static SessionFactory getSessionFactory() {
        return getSessionFactory(getConfiguration());
    }
    
    /**
     * Возвращает фабрику сессий гибернейта.
     * 
     * @param hibernateXmlFile - путь к файлу настроек
     * @return фабрику сессий гибернейта.
     */
    public static SessionFactory getSessionFactory(String hibernateXmlFile) {
        if (sessionFactory == null) {
            configuration = getConfiguration(hibernateXmlFile);
            
            return getSessionFactory(configuration);
        }
        
        LOG.debug("Запрошено создание новой фабрики сессий hibernate");

        return sessionFactory;
    }
    
    
    /**
     * Закрываем sessionFactory
     */
    public static synchronized void sessionFactoryClose() {
        if (sessionFactory != null) {
            sessionFactory.close();
            sessionFactory = null;
        }
    }
    
    /**
     * Возвращает фабрику соединений
     * 
     * @return фабрику соединений
     */
    public static synchronized ConnectionFactory getConnectionFactory() {
        // Чтение настроек о зарегистрированных пулах соединений и их
        // инициализация.
        
        if (connectionFactory == null) {
            connectionFactory = new HikariCPConnectionsFactory(
                    new HikariCPSettingsService(getSessionFactory()));
        }

        return connectionFactory;
    }

    /**
     * Закрываем connectionFactory
     */
    public static synchronized void connectionFactoryClose() {
        if (connectionFactory != null) {
            connectionFactory.close();
            connectionFactory = null;
        }
    }
    
    /**
     * Возвращает сервис настройки соединений
     * 
     * @return сервис настройки соединений
     */
    public static synchronized TaskSettingsService getTaskSettingsService() {
        // Чтение настроек о зарегистрированных пулах соединений и их
        // инициализация.
        if (taskSettingsService == null) {
            taskSettingsService = new TaskSettingsService(getSessionFactory());
        }

        return taskSettingsService;
    }
    
    /**
     * Закрываем сервис настройки соединений
     */
    public static void taskSettingsServiceClose() {
        taskSettingsService = null;
    }
    
    /**
     * Возвращает пул задач
     * 
     * @return пул задач
     */
    public static synchronized TasksPool getTasksPool() {
        // Чтение настроек о зарегистрированных пулах соединений и их
        // инициализация.
        if (tasksPool == null) {
            tasksPool = new TasksPool(getTaskSettingsService());
        }

        return tasksPool;
    }
    
    /**
     * Закрываем сервис настройки соединений
     */
    public static void tasksPoolClose() {
        tasksPool = null;
    }
    
    /**
     * Возвращает сервис настройки соединений
     * 
     * @return сервис настройки соединений
     */
    public static synchronized CronSettingsService getCronSettingsService() {
        // Чтение настроек о зарегистрированных пулах соединений и их
        // инициализация.
        if (cronSettingsService == null) {
            cronSettingsService = new CronSettingsService(getSessionFactory());
        }

        return cronSettingsService;
    }
    
    /**
     * Закрываем сервис настройки соединений
     */
    public static void cronSettingsServiceClose() {
        cronSettingsService = null;
    }
    
    /**
     * Возвращает пул задач
     * 
     * @return пул задач
     */
    public static synchronized CronPool getCronPool() {
        // Чтение настроек о зарегистрированных пулах соединений и их
        // инициализация.
        if (cronPool == null) {
            cronPool = new CronPool(getCronSettingsService());
        }

        return cronPool;
    }
    
    /**
     * Закрываем сервис настройки соединений
     */
    public static synchronized void cronPoolClose() {
        if (cronPool != null) {
            cronPool.stop();
            cronPool = null;
        }
    }
    
    /**
     * Возвращает пул потоков
     * 
     * @return пул потоков
     * @throws InterruptedException 
     */
    public static synchronized ThreadPool getThreadPool() {
        if (threadPool == null) {
            ApplicatonSettingsService aService = new ApplicatonSettingsService(sessionFactory);
            int count = Integer.parseInt(aService.getValue("tp.threads"));
            threadPool = new ThreadPool(count);
        }

        return threadPool;
    }
    
    /**
     * Закрываем пул потоков
     * @throws InterruptedException 
     */
    public static synchronized void threadPoolClose() throws InterruptedException {
        if(threadPool != null) {
            threadPool.shutdownThreadPool();
            threadPool = null;
        }
    }
    
    /**
     * Получаем сервис статистики
     * 
     * @return сервис статистики
     */
    public static synchronized StatsService getStatsService() {
        if (statsService == null) {
            ApplicatonSettingsService aService = new ApplicatonSettingsService(getSessionFactory());
            String baseConnName = aService.getValue("stats.dest");
            statsService = new StatsService(getConnectionFactory().get(baseConnName));
        }

        return statsService;
    }
    
    /**
     * Закрываем сервис статситики
     */
    public static synchronized void statsServiceClose() {
        statsService = null;
    }
    
    /**
     * Получаем сервис ErrorsLog
     * 
     * @return сервис ErrorsLog
     */
    public static synchronized ErrorsLog getErrorsLog() {
        ApplicatonSettingsService aService = new ApplicatonSettingsService(getSessionFactory());
        String baseConnName = aService.getValue("error.dest");

        return new ErrorsLog(getConnectionFactory().get(baseConnName));
    }
}
