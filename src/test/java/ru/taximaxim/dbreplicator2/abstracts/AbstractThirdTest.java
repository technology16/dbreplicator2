package ru.taximaxim.dbreplicator2.abstracts;

import java.io.IOException;
import java.sql.SQLException;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistryBuilder;

import ru.taximaxim.dbreplicator2.model.BoneCPDataBaseSettingsStorage;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsService;
import ru.taximaxim.dbreplicator2.utils.Core;

public abstract class AbstractThirdTest {
    
    protected static SessionFactory sessionFactory;
    protected static BoneCPDataBaseSettingsStorage settingStorage;
    
    protected static void setUp() throws ClassNotFoundException, SQLException, IOException {   
        Configuration configuration = new Configuration().configure();
        
        // Инициализируем Hibernate
        sessionFactory = new Configuration()
        .configure()
        .buildSessionFactory(new ServiceRegistryBuilder()
            .applySettings(configuration.getProperties()).buildServiceRegistry());

        // Инициализируем хранилище настроек пулов соединений
        settingStorage = new BoneCPSettingsService(sessionFactory);

    }
    
    /**
     * Закрытие соединений
     * @throws SQLException 
     * @throws InterruptedException 
     */
    protected static void close() throws SQLException, InterruptedException {
        if (sessionFactory != null) {
            sessionFactory.close();
        }
        Core.connectionFactoryClose();
        Core.sessionFactoryClose();
        Core.statsServiceClose();
        Core.tasksPoolClose();
        Core.taskSettingsServiceClose(); 
        Core.configurationClose();
    }

}
