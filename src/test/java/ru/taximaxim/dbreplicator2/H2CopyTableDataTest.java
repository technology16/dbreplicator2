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
import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.model.RunnerService;

public class H2CopyTableDataTest {
    protected static final Logger LOG = Logger.getLogger(H2CopyTableDataTest.class);
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
        connectionFactory.close();
        session.close();
        sessionFactory.close();
    }
    
    @Test
    public void testTableDataTest() throws SQLException, ClassNotFoundException, IOException {

        LOG.debug("Start: ");
        String source = "source";
        Connection conn = connectionFactory.getConnection(source);
        
        Helper.executeSqlFromFile(conn, "importRep2.sql");
        Helper.executeSqlFromFile(conn, "importSource.sql");
        createTrigger(conn);
        Helper.executeSqlFromFile(conn, "importSourceData.sql");
        

        String dest = "dest";
        Connection connDest = connectionFactory.getConnection(dest);
        Helper.executeSqlFromFile(connDest, "importDest.sql");
        
        RunnerService runnerService = new RunnerService(sessionFactory);
        
        Runnable worker = new WorkerThread(runnerService.getRunner(1));
        
        worker.run();
        
        int count_rep2_superlog = Helper.InfoCount(conn, "rep2_superlog");
        if(count_rep2_superlog!=0) {
            LOG.error("Таблица rep2_superlog должна быть пустой: count = " + count_rep2_superlog);
        }
        Assert.assertEquals(count_rep2_superlog, 0);

        int count_rep2_workpool_data = Helper.InfoCount(conn, "rep2_workpool_data");
        if(count_rep2_workpool_data!=0) {
            LOG.error("Таблица rep2_workpool_data должна быть пустой: count = " + count_rep2_workpool_data);
        }
        Assert.assertEquals(count_rep2_workpool_data, 0);
        
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);
//        LOG.info("<======Inception======>");
//        Helper.InfoList(listSource);
//        LOG.info("=======Inception=======");
//        Helper.InfoList(listDest);
//        LOG.info(">======Inception======<");
        
        
        
        Helper.executeSqlFromFile(conn, "sql_insert.sql");   
        worker.run();
        listSource = Helper.InfoTest(conn, "t_table");
        listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
        
        
        
        Helper.executeSqlFromFile(conn, "sql_update.sql");   
        worker.run();
        listSource = Helper.InfoTest(conn, "t_table");
        listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
        
        
        
        Helper.executeSqlFromFile(conn, "sql_delete.sql");   
        worker.run();
        listSource = Helper.InfoTest(conn, "t_table");
        listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);

        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
        
        
        
        
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");   
        worker.run();
        worker.run();
        listSource = Helper.InfoTest(conn, "t_table2");
        listDest   = Helper.InfoTest(connDest, "t_table2");
        Helper.AssertEquals(listSource, listDest);
//
        listSource = Helper.InfoTest(conn, "t_table3");
        listDest   = Helper.InfoTest(connDest, "t_table3");
        Helper.AssertEquals(listSource, listDest);
        
        
        conn.close();
        connDest.close();
    }
    
    /**
     * Создание триггера
     */
    public void createTrigger(Connection conn)
            throws SQLException, ClassNotFoundException {
        Helper.createTrigger(conn, "t_table");
        Helper.createTrigger(conn, "t_table1");
        Helper.createTrigger(conn, "t_table2");
        Helper.createTrigger(conn, "t_table3");
    }
}
