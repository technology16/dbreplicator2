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

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.model.RunnerService;

/**
 * Тест соединения с базой данных по протоколу TCP.
 * 
 * @author ags
 */
public class H2CreateSetting {

    protected static final Logger LOG = Logger.getLogger(H2CreateSetting.class);
    protected static SessionFactory sessionFactory;
    protected static Session session;
    protected static ConnectionFactory connectionFactory;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        sessionFactory = Application.getSessionFactory();
        session = sessionFactory.openSession();
        connectionFactory = Application.getConnectionFactory();
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        
    }
    
    @Test
    public void testConnection() throws SQLException, ClassNotFoundException, IOException {

        LOG.debug("Start: ");
        String source = "source";
        Connection conn = connectionFactory.getConnection(source);
        
        Helper.executeSqlFromFile(conn, "importRep2.sql");
        Helper.executeSqlFromFile(conn, "importSource.sql");
        createTrigger(conn);
        Helper.executeSqlFromFile(conn, "importSourceData.sql");
        
        LOG.info("<======Inception======>");
        Helper.InfoSuperLog(conn);
        LOG.info(">======Inception======<");
        
        RunnerService runnerService = new RunnerService(sessionFactory);
        
        Runnable worker = new WorkerThread(runnerService.getRunner(1));
        
        worker.run();
        
        LOG.info("<====== RESULT ======>");
        Helper.InfoSuperLog(conn);
        LOG.info("======= RESULT =======");
        Helper.InfoWorkPoolData(conn);
        LOG.info(">====== RESULT ======<");
        conn.close();
    }
    
    /**
     * Создание триггера
     */
    public void createTrigger(Connection conn)
            throws SQLException, ClassNotFoundException {
        Helper.createTrigger(conn, "T_TABLE1");
        Helper.createTrigger(conn, "T_TABLE2");
        Helper.createTrigger(conn, "T_TABLE3");
        Helper.createTrigger(conn, "T_TABLE4");
        Helper.createTrigger(conn, "T_TABLE5");
        Helper.createTrigger(conn, "T_TABLE6");
        Helper.createTrigger(conn, "T_TABLE7");
        Helper.createTrigger(conn, "T_TABLE8");
        Helper.createTrigger(conn, "T_TABLE9");
        Helper.createTrigger(conn, "T_TABLE0");
    }
}
