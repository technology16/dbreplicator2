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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.jdbc.Jdbc;
import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.model.RunnerService;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Тест репликации данных между базами H2-H2. 
 * 
 * Данный тест использует асинхронный менеджер записей супер лог таблицы, 
 * поэтому после каждого цикла репликации вызывается инструкция 
 * Thread.sleep(500); Тест может некорректно работать на медленных машинах, 
 * при необходимости подгонять величину задержки вручную!
 * 
 * @author volodin_aa
 *
 */
public class H2CopyTableDataTest2rep {
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 500;
    
    protected static final Logger LOG = Logger.getLogger(H2CopyTableDataTest.class);
    protected static SessionFactory sessionFactory;
    protected static Session session;
    protected static ConnectionFactory connectionFactory;
    protected static Connection conn = null;
    protected static Connection connDest = null;
    protected static Runnable worker = null;
    protected static Runnable worker2 = null;
    protected static Runnable errorsCountWatchdogWorker = null;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        sessionFactory = Core.getSessionFactory();
        session = sessionFactory.openSession();
        connectionFactory = Core.getConnectionFactory();
        initialization();
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        if(conn!=null)
            conn.close();
        if(connDest!=null)
            connDest.close();
        if(session!=null)
            session.close();
        Core.connectionFactoryClose();
        Core.sessionFactoryClose();
        Core.statsServiceClose();
    }
    
    /**
     * Проверка внешних ключей
     * вставка в главную таблицу  
     * вставка таблицу подчиненную
     * изменение главной таблицы
     * 
     * репликация
     * 
     * вставка таблицу подчиненную
     * изменение главной таблицы
     * 
     * 
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException 
     */
    @Test
    public void testForeignKey() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        //Проверка внешних ключей
        LOG.info("Проверка внешних ключей");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        
        workerRun();
        Helper.executeSqlFromFile(connDest,  "sql_foreign_key2.sql");
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        // Выводим данные из rep2_superlog_table
        try (PreparedStatement select = 
                conn.prepareStatement("SELECT * FROM rep2_workpool_data");
        ) {
            ResultSet result = select.executeQuery();
            List<String> cols = 
                    new ArrayList<String>(JdbcMetadata.getColumns(conn, "REP2_WORKPOOL_DATA"));
            while (result.next()) {
                LOG.info(Jdbc.resultSetToString(result, cols));
            }
        }
        
        errorsCountWatchdogWorker.run();
        workerRun();
        Thread.sleep(REPLICATION_DELAY);
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table2");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table2");
        Helper.AssertEquals(listSource, listDest);

        listSource = Helper.InfoTest(conn, "t_table3");
        listDest   = Helper.InfoTest(connDest, "t_table3");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table2");
        listDest   = Helper.InfoTest(connDest, "t_table2");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table3");
        listDest   = Helper.InfoTest(connDest, "t_table3");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table4");
        listDest   = Helper.InfoTest(connDest, "t_table4");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table5");
        listDest   = Helper.InfoTest(connDest, "t_table5");
        Helper.AssertEquals(listSource, listDest);
        
        workerEnd();
        workerEnd2();
        
        int count = Helper.InfoCount(conn,  "rep2_superlog");
        assertTrue(String.format("Количество записей должно быть пустым [%s == 0]", count), 0 == count);
    }    
    
    /**
     * Проверка обновления
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException 
     */
    @Test
    public void testUpdate() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        testInsert();
        //Проверка обновления
        LOG.info("Проверка обновления");
        Helper.executeSqlFromFile(conn, "sql_update.sql");   
        workerRun();
        Helper.executeSqlFromFile(connDest,  "sql_update2.sql");     
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table2");
        listDest   = Helper.InfoTest(connDest, "t_table2");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table3");
        listDest   = Helper.InfoTest(connDest, "t_table3");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table4");
        listDest   = Helper.InfoTest(connDest, "t_table4");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table5");
        listDest   = Helper.InfoTest(connDest, "t_table5");
        Helper.AssertEquals(listSource, listDest);
        
        workerEnd();
        workerEnd2();
        
        int count = Helper.InfoCount(conn,  "rep2_superlog");
        assertTrue(String.format("Количество записей должно быть пустым [%s == 0]", count), 0 == count);
    }
    
    /**
     * Проверка удаления
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException 
     */
    @Test
    public void testDelete() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        testInsert();
        LOG.info("Проверка удаления");
        //Проверка удаления
        Helper.executeSqlFromFile(conn, "sql_delete.sql");   
        workerRun();
        Helper.executeSqlFromFile(connDest, "sql_delete.sql");    
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);

        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table2");
        listDest   = Helper.InfoTest(connDest, "t_table2");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table3");
        listDest   = Helper.InfoTest(connDest, "t_table3");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table4");
        listDest   = Helper.InfoTest(connDest, "t_table4");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table5");
        listDest   = Helper.InfoTest(connDest, "t_table5");
        Helper.AssertEquals(listSource, listDest);
        
        workerEnd();
        workerEnd2();
        
        int count = Helper.InfoCount(conn,  "rep2_superlog");
        assertTrue(String.format("Количество записей должно быть пустым [%s == 0]", count), 0 == count);
    }
    
    /**
     * Проверка вставки и обновления
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException 
     */
    @Test//error//TODO
    public void testInsertUpdate() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
      //Проверка вставки и обновления
        LOG.info("Проверка вставки и обновления");
        Helper.executeSqlFromFile(conn, "sql_insert.sql");   
        Helper.executeSqlFromFile(conn, "sql_update.sql");   
        workerRun();
        //Helper.executeSqlFromFile(connDest, "sql_insert.sql");   
        Helper.executeSqlFromFile(connDest, "sql_update2.sql");  
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table2");
        listDest   = Helper.InfoTest(connDest, "t_table2");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table3");
        listDest   = Helper.InfoTest(connDest, "t_table3");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table4");
        listDest   = Helper.InfoTest(connDest, "t_table4");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table5");
        listDest   = Helper.InfoTest(connDest, "t_table5");
        Helper.AssertEquals(listSource, listDest);
        
        workerEnd();
        workerEnd2();
        
        int count = Helper.InfoCount(conn,  "rep2_superlog");
        assertTrue(String.format("Количество записей должно быть пустым [%s == 0]", count), 0 == count);
    }
    
    /**
     * Проверка вставки и удаления
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException 
     */
    @Test
    public void testInsertDelete() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        LOG.info("Проверка вставки и удаления");
      //Проверка вставки и удаления
        Helper.executeSqlFromFile(conn, "sql_insert.sql");   
        Helper.executeSqlFromFile(conn, "sql_delete.sql");   
        workerRun();
        Helper.executeSqlFromFile(connDest, "sql_insert.sql");   
        Helper.executeSqlFromFile(connDest, "sql_delete.sql");   
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);

        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table2");
        listDest   = Helper.InfoTest(connDest, "t_table2");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table3");
        listDest   = Helper.InfoTest(connDest, "t_table3");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table4");
        listDest   = Helper.InfoTest(connDest, "t_table4");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table5");
        listDest   = Helper.InfoTest(connDest, "t_table5");
        Helper.AssertEquals(listSource, listDest);
        
        workerEnd();
        workerEnd2();
        
        int count = Helper.InfoCount(conn,  "rep2_superlog");
        assertTrue(String.format("Количество записей должно быть пустым [%s == 0]", count), 0 == count);
    }
    
    /**
     * Инициализация
     */
    public static void initialization() throws ClassNotFoundException, SQLException, IOException{
        LOG.info("initialization");
        String source = "source";
        conn = connectionFactory.getConnection(source);
        
        Helper.executeSqlFromFile(conn, "importRep2.sql");
        Helper.executeSqlFromFile(conn, "importSource.sql");
        
        String dest = "dest";
        connDest = connectionFactory.getConnection(dest);
        Helper.executeSqlFromFile(connDest, "importRep2.sql");
        Helper.executeSqlFromFile(connDest, "importDest.sql");
        
        RunnerService runnerService = new RunnerService(sessionFactory);

        worker = new WorkerThread(runnerService.getRunner(1));
        worker2 = new WorkerThread(runnerService.getRunner(2));
        errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(6));
    }

    /**
     * Проверка вставки 
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException 
     */
    @Test
    public void testInsert() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
      //Проверка вставки
        Helper.executeSqlFromFile(conn, "sql_insert.sql");
        workerRun();
        //Helper.executeSqlFromFile(connDest, "sql_insert.sql"); 
        workerRun2();
        
       // Helper.executeSqlFromSql(conn, "UPDATE T_TAB SET _value = ?", "source");
       // Helper.executeSqlFromSql(conn, "UPDATE T_TAB SET _value = ?", "dest");
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();
        
        workerRun();
        workerRun2();

        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table2");
        listDest   = Helper.InfoTest(connDest, "t_table2");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table3");
        listDest   = Helper.InfoTest(connDest, "t_table3");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table4");
        listDest   = Helper.InfoTest(connDest, "t_table4");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table5");
        listDest   = Helper.InfoTest(connDest, "t_table5");
        Helper.AssertEquals(listSource, listDest);
        
        workerEnd();
        workerEnd2();        
        int count = Helper.InfoCount(conn,  "rep2_superlog");
        assertTrue(String.format("Количество записей должно быть пустым [%s == 0]", count), 0 == count);
    }
    
    public void workerRun() throws IOException, SQLException, InterruptedException{
        worker.run();
        Helper.executeSqlFromSql(conn, "UPDATE T_TAB SET _value = ?", "dest");
        Thread.sleep(REPLICATION_DELAY);
    }
    
    public void workerRun2() throws IOException, SQLException, InterruptedException{
        worker2.run();
        Helper.executeSqlFromSql(connDest, "UPDATE T_TAB SET _value = ?", "source");
        Thread.sleep(REPLICATION_DELAY);
    }
    
    public void workerEnd() throws IOException, SQLException, InterruptedException{
        Helper.executeSqlFromSql(conn, "UPDATE T_TAB SET _value = ?", "");
    }
    
    public void workerEnd2() throws IOException, SQLException, InterruptedException{
        Helper.executeSqlFromSql(connDest, "UPDATE T_TAB SET _value = ?", "");
    }
}
