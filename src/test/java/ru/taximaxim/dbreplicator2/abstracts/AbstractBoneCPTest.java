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

package ru.taximaxim.dbreplicator2.abstracts;

import java.io.IOException;
import java.sql.SQLException;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistryBuilder;

import ru.taximaxim.dbreplicator2.model.BoneCPDataBaseSettingsStorage;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsService;
import ru.taximaxim.dbreplicator2.utils.Core;


/**
 * Абстракный класс для инициализации общих полей тестовых классов
 * 
 * @author petrov_im
 *
 */
public abstract class AbstractBoneCPTest {
    
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
