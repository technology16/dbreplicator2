/**
 * 
 */
package ru.taximaxim.dbreplicator2.utils;

import java.io.File;

import org.apache.log4j.Logger;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;

import ru.taximaxim.dbreplicator2.cf.BoneCPConnectionsFactory;
import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsService;
import ru.taximaxim.dbreplicator2.model.TaskSettingsService;
import ru.taximaxim.dbreplicator2.tasks.TasksPool;


/**
 * @author volodin_aa
 *
 */
public final class Core {
    
    private static final Logger LOG = Logger.getLogger(Core.class);

    /**
     * Данный класс нельзя инстанциировать.
     */
    private Core() {
    }

    private static SessionFactory sessionFactory;

    private static ConnectionFactory connectionFactory;
    
    private static TaskSettingsService taskSettingsService;
    
    private static TasksPool tasksPool;
    
    private static Configuration configuration;

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
     * Возвращает фабрику сессий гибернейта.
     * 
     * @param configuration - инициализированная конфигурация
     * @return фабрику сессий гибернейта.
     */
    public static synchronized SessionFactory getSessionFactory(Configuration configuration) {
        LOG.debug("Запрошено создание новой фабрики сессий hibernate");

        if (sessionFactory == null) {
            ServiceRegistry serviceRegistry = new ServiceRegistryBuilder()
                    .applySettings(configuration.getProperties())
                    .buildServiceRegistry();
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
    public static void sessionFactoryClose() {
        sessionFactory.close();
        sessionFactory = null;
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

}
