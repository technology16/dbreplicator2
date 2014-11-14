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
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

import ru.taximaxim.dbreplicator2.Helper;
import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.stats.StatsService;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Абстракный класс для инициализации общих полей тестовых классов
 * 
 * @author petrov_im
 *
 */
public abstract class AbstractSettingTest {
    
    protected static final Logger LOG = Logger.getLogger(AbstractSettingTest.class);

    protected static SessionFactory sessionFactory;
    protected static Session session;
    protected static ConnectionFactory connectionFactory;
    protected static StatsService statsService;
    protected static Connection conn = null;

    protected static void setUp(String sqlRep2, String sqlSourse, String sqlDest) throws ClassNotFoundException, SQLException, IOException {
        sessionFactory = Core.getSessionFactory();
        session = sessionFactory.openSession();
        connectionFactory = Core.getConnectionFactory();
        statsService = Core.getStatsService();
        
        String source = "source";
        conn = connectionFactory.getConnection(source);
        
        if (sqlRep2 != null)
            Helper.executeSqlFromFile(conn, sqlRep2);
        if (sqlSourse != null)
            Helper.executeSqlFromFile(conn, sqlSourse);
        if (sqlDest != null)
            Helper.executeSqlFromFile(conn, sqlDest);
        
    }

    /**
     * Закрытие соединений
     * @throws SQLException 
     * @throws InterruptedException 
     */
    protected static void close() throws SQLException, InterruptedException {
        if (session != null)
            session.close();
        if (conn != null)
            conn.close();
        connectionFactory.close();
        sessionFactory.close();
        Core.connectionFactoryClose();
        Core.sessionFactoryClose();
        Core.statsServiceClose();
        Core.threadPoolClose();
        Core.tasksPoolClose();
        Core.taskSettingsServiceClose(); 
        Core.configurationClose();
    }

}
