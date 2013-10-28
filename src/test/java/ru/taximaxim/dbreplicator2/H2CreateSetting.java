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
import java.sql.Statement;

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
    public void testConnection() throws SQLException, ClassNotFoundException {

        LOG.debug("Start: ");
        String source = "source";
        Connection conn = connectionFactory.getConnection(source);
        
        Helper.CreateTebleRep2(conn);
        CreateTeble(conn);

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
    
    public void CreateTeble(Connection conn)
            throws SQLException, ClassNotFoundException {

        Statement stat = conn.createStatement();

        Helper.delete(stat, "T_TABLE1");
        Helper.delete(stat, "T_TABLE2");
        Helper.delete(stat, "T_TABLE3");
        Helper.delete(stat, "T_TABLE4");
        Helper.delete(stat, "T_TABLE5");
        Helper.delete(stat, "T_TABLE6");
        Helper.delete(stat, "T_TABLE7");
        Helper.delete(stat, "T_TABLE8");
        Helper.delete(stat, "T_TABLE9");
        Helper.delete(stat, "T_TABLE0");
        
        stat.execute("CREATE TABLE T_TABLE1(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        Helper.createTrigger(conn, "T_TABLE1");

        stat.execute("CREATE TABLE T_TABLE2(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        Helper.createTrigger(conn, "T_TABLE2");

        stat.execute("CREATE TABLE T_TABLE3(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        Helper.createTrigger(conn, "T_TABLE3");

        stat.execute("CREATE TABLE T_TABLE4(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        Helper.createTrigger(conn, "T_TABLE4");

        stat.execute("CREATE TABLE T_TABLE5(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        Helper.createTrigger(conn, "T_TABLE5");

        stat.execute("CREATE TABLE T_TABLE6(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        Helper.createTrigger(conn, "T_TABLE6");

        stat.execute("CREATE TABLE T_TABLE7(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        Helper.createTrigger(conn, "T_TABLE7");

        stat.execute("CREATE TABLE T_TABLE8(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        Helper.createTrigger(conn, "T_TABLE8");

        stat.execute("CREATE TABLE T_TABLE9(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        Helper.createTrigger(conn, "T_TABLE9");

        stat.execute("CREATE TABLE T_TABLE0(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        Helper.createTrigger(conn, "T_TABLE0");
        
        InsertInto(conn);
    }

    public void InsertInto(Connection conn)
            throws SQLException, ClassNotFoundException {

        Statement stat = conn.createStatement();
        try {

            stat.execute("INSERT INTO T_TABLE1 VALUES(1, 10.00, 'TESTER')");
            stat.execute("INSERT INTO T_TABLE1 VALUES(2, 19.95, 'TESTER')");
            stat.execute("UPDATE T_TABLE1 SET C_AMOUNT = 20.0 WHERE ID = 2");
            stat.execute("DELETE FROM T_TABLE1 WHERE ID = 1");

            stat.execute("INSERT INTO T_TABLE2 VALUES(1, 14.50, 'TESTER')");
            stat.execute("INSERT INTO T_TABLE2 VALUES(2, 12.55, 'TESTER')");
            stat.execute("UPDATE T_TABLE2 SET C_AMOUNT = 60.0 WHERE ID = 2");
            stat.execute("DELETE FROM T_TABLE2 WHERE ID = 1");

            stat.execute("INSERT INTO T_TABLE3 VALUES(1, 05.00, 'TESTER')");
            stat.execute("INSERT INTO T_TABLE3 VALUES(2, 78.55, 'TESTER')");
            stat.execute("UPDATE T_TABLE3 SET C_AMOUNT = 67.99 WHERE ID = 2");
            stat.execute("DELETE FROM T_TABLE3 WHERE ID = 1");

            stat.execute("INSERT INTO T_TABLE4 VALUES(1, 37.00, 'TESTER')");
            stat.execute("INSERT INTO T_TABLE4 VALUES(2, 13.88, 'TESTER')");
            stat.execute("UPDATE T_TABLE4 SET C_AMOUNT = 23.78 WHERE ID = 2");
            stat.execute("DELETE FROM T_TABLE4 WHERE ID = 1");

            stat.execute("INSERT INTO T_TABLE5 VALUES(1, 86.00, 'TESTER')");
            stat.execute("INSERT INTO T_TABLE5 VALUES(2, 99.99, 'TESTER')");
            stat.execute("UPDATE T_TABLE5 SET C_AMOUNT = 10.09 WHERE ID = 2");
            stat.execute("DELETE FROM T_TABLE5 WHERE ID = 1");

            stat.execute("INSERT INTO T_TABLE6 VALUES(1, 44.55, 'TESTER')");
            stat.execute("INSERT INTO T_TABLE6 VALUES(2, 36.47, 'TESTER')");
            stat.execute("UPDATE T_TABLE6 SET C_AMOUNT = 79.80 WHERE ID = 2");
            stat.execute("DELETE FROM T_TABLE6 WHERE ID = 1");

        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
