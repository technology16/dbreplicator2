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
import org.hibernate.SessionFactory;
import org.hibernate.Session;
import org.hibernate.cfg.Configuration;

import ru.taximaxim.dbreplicator2.Helper;
import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.el.ErrorsLog;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Абстракный класс для инициализации общих полей тестовых классов
 * 
 * @author petrov_im
 *
 */
public abstract class AbstractReplicationTest {
    
    protected static final Logger LOG = Logger.getLogger(AbstractReplicationTest.class);
    
    protected static SessionFactory sessionFactory;
    protected static Session session;
    protected static ConnectionFactory connectionFactory;
    protected static ErrorsLog errorsLog; 
    protected static Connection conn = null;
    protected static Connection connDest = null;
    protected static Runnable worker = null;
    protected static Runnable worker2 = null;
    protected static Runnable errorsCountWatchdogWorker = null;

    protected static void setUp(String importSql, String sqlRep2, String sqlSourse, String sqlDest) throws ClassNotFoundException, SQLException, IOException {
        setUp("src/test/resources/hibernate.cfg.xml", importSql, sqlRep2, sqlSourse, sqlDest);

    }
    
    protected static void setUp(String xmlForConfig, String importSql, String sqlRep2, String sqlSourse, String sqlDest) throws ClassNotFoundException, SQLException, IOException {
        Core.configurationClose();
        
        if(xmlForConfig != null){
            Configuration configuration = Core.getConfiguration(xmlForConfig);
            if (importSql != null) {
                configuration.setProperty("hibernate.hbm2ddl.import_files", importSql);
            }
            sessionFactory = Core.getSessionFactory(configuration);
        }
        else {
            sessionFactory = Core.getSessionFactory();
        }
        connectionFactory = Core.getConnectionFactory();
        session = sessionFactory.openSession();
        errorsLog = Core.getErrorsLog();
        initialization(sqlRep2, sqlSourse, sqlDest);
    }
    
    /**
     * Закрытие соединений
     * @throws SQLException 
     * @throws InterruptedException 
     */
    protected static void close() throws SQLException, InterruptedException {
        if(conn!=null)
            conn.close();
        if(connDest!=null)
            connDest.close();
        if(session!=null)
            session.close();
        sessionFactory.close();
        connectionFactory.close();
        sessionFactory.close();
        Core.connectionFactoryClose();
        Core.sessionFactoryClose();
        Core.statsServiceClose();
        Core.tasksPoolClose();
        Core.taskSettingsServiceClose();
        Core.configurationClose();
        Core.threadPoolClose();
        errorsLog.close();
    }
    
    /**
     * Инициализация
     */
    public static void initialization(String sqlRep2, String sqlSourse, String sqlDest) throws ClassNotFoundException, SQLException, IOException{
        LOG.info("initialization");
        String source = "source";
        conn = connectionFactory.getConnection(source);

        Helper.executeSqlFromFile(conn, sqlRep2);
        Helper.executeSqlFromFile(conn, sqlSourse);

        String dest = "dest";
        connDest = connectionFactory.getConnection(dest);
        Helper.executeSqlFromFile(connDest, sqlRep2);
        Helper.executeSqlFromFile(connDest, sqlDest);
    }

}

