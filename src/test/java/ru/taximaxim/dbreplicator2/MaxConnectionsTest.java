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

import java.sql.Connection;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistryBuilder;

import ru.taximaxim.dbreplicator2.cf.BoneCPConnectionsFactory;
import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.model.BoneCPDataBaseSettingsStorage;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsModel;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsService;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Класс для тестирования пулов соединений
 *
 * @author volodin_aa
 *
 */
public class MaxConnectionsTest extends TestCase {

    private SessionFactory sessionFactory;
    
    // Хранилище настроек
    protected BoneCPDataBaseSettingsStorage settingStorage;

    @Override
    protected void setUp() throws Exception {
        Configuration configuration = new Configuration().configure();
        
        // Инициализируем Hibernate
        sessionFactory = new Configuration()
        .configure()
        .buildSessionFactory(new ServiceRegistryBuilder()
            .applySettings(configuration.getProperties()).buildServiceRegistry());

        // Инициализируем хранилище настроек пулов соединений
        settingStorage = new BoneCPSettingsService(sessionFactory);

        settingStorage.setDataBaseSettings(new BoneCPSettingsModel("1",
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test", "sa", ""));

        settingStorage.setDataBaseSettings(new BoneCPSettingsModel("2",
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test", "sa", ""));

        settingStorage.setDataBaseSettings(new BoneCPSettingsModel("3",
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test", "sa", ""));

        settingStorage.setDataBaseSettings(new BoneCPSettingsModel("4",
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test", "sa", "",
                1, 3, 1, 10000, 0));
    }

    @Override
    protected void tearDown() throws Exception {
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

    /**
     * Тест таймаута при привышении максимального количества открытых соединений
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public void testMaxConnections() throws ClassNotFoundException,
            SQLException {
        ConnectionFactory connectionsFactory = new BoneCPConnectionsFactory(
                settingStorage);
        try {
            Connection connection1 = connectionsFactory.getConnection("1");
            connection1.setAutoCommit(false);
            connection1.commit();

            Connection connection2 = connectionsFactory.getConnection("2");
            connection2.setAutoCommit(false);
            connection2.commit();

            Connection connection3 = connectionsFactory.getConnection("3");
            connection3.setAutoCommit(false);
            connection3.commit();

            Connection connection4 = connectionsFactory.getConnection("4");
            connection4.setAutoCommit(false);
            connection4.commit();

            Connection connection42 = connectionsFactory.getConnection("4");
            connection42.setAutoCommit(false);
            connection42.commit();

            Connection connection43 = connectionsFactory.getConnection("4");
            connection43.setAutoCommit(false);
            connection43.commit();

            Connection connection44 = connectionsFactory.getConnection("4");
            connection44.setAutoCommit(false);
            connection44.commit();

            connection1.close();
            connection2.close();
            connection3.close();
            connection4.close();
            connection42.close();
            connection43.close();
            connection44.close();
        } catch (SQLException e) {
            // Поглощаем только ошибку таймаута
            if (!e.getSQLState().equals("08001")) {
                throw e;
            }
        } finally {
            connectionsFactory.close();
        }

    }

}
