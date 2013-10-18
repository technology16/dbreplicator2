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

import org.h2.tools.Server;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import ru.taximaxim.dbreplicator2.cf.BoneCPConnectionsFactory;
import ru.taximaxim.dbreplicator2.cf.BoneCPDataBaseSettingsStorage;
import ru.taximaxim.dbreplicator2.cf.ConnectionsFactory;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsService;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsImpl;
/**
 * Класс для тестирования пулов соединений
 * 
 * @author volodin_aa
 *
 */
public class MaxConnectionsTest extends TestCase {
  
    private SessionFactory sessionFactory;
    private Server server;
    
    // Хранилище настроек
    protected BoneCPDataBaseSettingsStorage settingStorage;

    @Override
    protected void setUp() throws Exception {
        // Инициализируем БД настроек
        server = Server.createTcpServer(
                new String[] { "-tcpPort", "8084", "-tcpAllowOthers" }).start();
        
        // Инициализируем Hibernate
        sessionFactory = new Configuration()
            .configure()
            .buildSessionFactory();

        // Инициализируем хранилище настроек пулов соединений
        settingStorage = new BoneCPSettingsService(sessionFactory);
        
        settingStorage.setDataBaseSettings( new BoneCPSettingsImpl("1", 
                "org.postgresql.Driver", 
                "jdbc:postgresql://127.0.0.1:5432/LoadPullPgMsPub", 
                "ags", 
                ""));
        
        settingStorage.setDataBaseSettings( new BoneCPSettingsImpl("2", 
                "org.postgresql.Driver", 
                "jdbc:postgresql://127.0.0.1:5432/LoadPullPgMsPub", 
                "ags", 
                ""));
        
        settingStorage.setDataBaseSettings( new BoneCPSettingsImpl("3", 
                "org.postgresql.Driver", 
                "jdbc:postgresql://127.0.0.1:5432/LoadPullPgMsPub", 
                "ags", 
                ""));
        
        settingStorage.setDataBaseSettings( new BoneCPSettingsImpl("4", 
                "org.postgresql.Driver", 
                "jdbc:postgresql://127.0.0.1:5432/LoadPullPgMsPub", 
                "ags", 
                "",
                1,
                3,
                1,
                10000, 
                0));
    }

    @Override
    protected void tearDown() throws Exception {
        if ( sessionFactory != null ) {
            sessionFactory.close();
        }
        
        server.shutdown();
    }

    /** 
     * Тест таймаута при привышении максимального количества открытых соединений
     * 
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    public void testMaxConnections() throws ClassNotFoundException, SQLException {
        ConnectionsFactory connectionsFactory = new BoneCPConnectionsFactory(settingStorage);
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
        } catch(SQLException e){
            // Поглощаем только ошибку таймаута
            if (!e.getSQLState().equals("08001")){
                throw e;
            }
        } finally {
            connectionsFactory.close();
        }

    }
    
}
