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

package ru.taximaxim.dbreplicator2;

import org.apache.log4j.Logger;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;

import ru.taximaxim.dbreplicator2.cf.BoneCPConnectionsFactory;
import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.cli.CommandLineParser;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsService;
import ru.taximaxim.dbreplicator2.model.TaskSettingsService;
import ru.taximaxim.dbreplicator2.tasks.TasksPool;

/**
 * @author TaxiMaxim
 * 
 */
public final class Application {

    private static final Logger LOG = Logger.getLogger(Application.class);

    private static SessionFactory sessionFactory;

    private static ConnectionFactory connectionFactory;
    
    private static TaskSettingsService taskSettingsService;
    
    private static TasksPool tasksPool;

    /**
     * Данный класс нельзя инстанциировать.
     */
    private Application() {
    }

    /**
     * Возвращает фабрику сессий гибернейта.
     * 
     * @return фабрику сессий гибернейта.
     */
    public static SessionFactory getSessionFactory() {
        LOG.debug("Запрошено создание новой фабрики сессий hibernate");

        if (sessionFactory == null) {
            Configuration configuration = new Configuration();
            configuration.configure();

            ServiceRegistry serviceRegistry = new ServiceRegistryBuilder()
                    .applySettings(configuration.getProperties())
                    .buildServiceRegistry();
            sessionFactory = configuration.buildSessionFactory(serviceRegistry);

            LOG.info("Создана новая фабрика сессий hibernate");
        }
        return sessionFactory;
    }
    
    /**
     * Закрываем sessionFactory
     */
    public static void sessionFactoryClose() {
        sessionFactory.close();
        sessionFactory = null;
    }
    
    /**
     * Возвращает фабрику соединений
     * 
     * @return фабрику соединений
     */
    public static ConnectionFactory getConnectionFactory() {
        // Чтение настроек о зарегистрированных пулах соединений и их
        // инициализация.
        
        if (connectionFactory == null) {
            connectionFactory = new BoneCPConnectionsFactory(
                    new BoneCPSettingsService(getSessionFactory()));
        }

        return connectionFactory;
    }

    /**
     * Закрываем connectionFactory
     */
    public static void connectionFactoryClose() {
        connectionFactory.close();
        connectionFactory = null;
    }
    
    /**
     * Возвращает сервис настройки соединений
     * 
     * @return сервис настройки соединений
     */
    public static TaskSettingsService getTaskSettingsService() {
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
    public static TasksPool getTasksPool() {
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
    
    public static void main(String[] args) {
//        CommandLineParser.parse(args);
        CommandLineParser.parse(new String[]{"-i", "-s"});

        // Определение рабочих потоков, подготовка пула потоков.
        // 1. Расширить таблицы H2 насторйками пулов рабочих потоков.
        // 2. Инициализация пулов потоков.
    }
}
