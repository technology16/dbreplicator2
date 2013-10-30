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
    protected static Connection conn = null;
    protected static Connection connDest = null;
    protected static Runnable worker = null;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        sessionFactory = Application.getSessionFactory();
        session = sessionFactory.openSession();
        connectionFactory = Application.getConnectionFactory();
        initialization();
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        conn.close();
        connDest.close();
        session.close();
        connectionFactory.close();
        Application.connectionFactoryClose();
        sessionFactory.close();
        Application.sessionFactoryClose();
    }
    

    /**
     * Проверка Супер логов
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    @Test
    public void testTableDataTest() throws SQLException, ClassNotFoundException, IOException {

        Helper.executeSqlFromFile(conn, "importSourceData.sql");
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
        
        LOG.info("<======Inception======>");
        Helper.InfoList(listSource);
        LOG.info("=======Inception=======");
        Helper.InfoList(listDest);
        LOG.info(">======Inception======<");
    }
    
    /**
     * Тестирование null значений в разных типах
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    @Test
    public void testNull() throws SQLException, ClassNotFoundException, IOException {
        //Проверка null
        LOG.info("Проверка null");
        Helper.executeSqlFromFile(conn, "sql_null.sql");   
        worker.run();
        
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table4");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table4");
        
        Helper.AssertEquals(listSource, listDest);
//
        listSource = Helper.InfoTest(conn, "t_table5");
        listDest   = Helper.InfoTest(connDest, "t_table5");
        Helper.AssertEquals(listSource, listDest);
        
        Helper.InfoNull(conn, "t_table4", 1);
        Helper.InfoNull(conn, "t_table5", 2);

        Helper.InfoNull(connDest, "t_table4", 1);
        Helper.InfoNull(connDest, "t_table5", 2);
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
     */
    @Test
    public void testForeignKey() throws SQLException, ClassNotFoundException, IOException {
        //Проверка внешних ключей
        LOG.info("Проверка внешних ключей");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");   
        worker.run();
        worker.run();
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table2");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table2");
        Helper.AssertEquals(listSource, listDest);
//
        listSource = Helper.InfoTest(conn, "t_table3");
        listDest   = Helper.InfoTest(connDest, "t_table3");
        Helper.AssertEquals(listSource, listDest);
    }
    
    /**
     * Проверка вставки 
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    @Test
    public void testInsert() throws SQLException, ClassNotFoundException, IOException {
      //Проверка вставки
        Helper.executeSqlFromFile(conn, "sql_insert.sql");   
        worker.run();
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
    }
    
    /**
     * Проверка обновления
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    @Test
    public void testUpdate() throws SQLException, ClassNotFoundException, IOException {
        testInsert();
        //Проверка обновления
        LOG.info("Проверка обновления");
        Helper.executeSqlFromFile(conn, "sql_update.sql");   
        worker.run();
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
    }
    
    /**
     * Проверка удаления
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    @Test
    public void testDelete() throws SQLException, ClassNotFoundException, IOException {
        testInsert();
        LOG.info("Проверка удаления");
        //Проверка удаления
        Helper.executeSqlFromFile(conn, "sql_delete.sql");   
        worker.run();
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);

        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
    }
    
    /**
     * Проверка вставки и обновления
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    @Test
    public void testInsertUpdate() throws SQLException, ClassNotFoundException, IOException {
      //Проверка вставки и обновления
        LOG.info("Проверка вставки и обновления");
        Helper.executeSqlFromFile(conn, "sql_insert.sql");   
        Helper.executeSqlFromFile(conn, "sql_update.sql");   
        worker.run();
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
    }
    
    /**
     * Проверка вставки и удаления
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     */
    @Test
    public void testInsertDelete() throws SQLException, ClassNotFoundException, IOException {
        LOG.info("Проверка вставки и удаления");
      //Проверка вставки и удаления
        Helper.executeSqlFromFile(conn, "sql_insert.sql");   
        Helper.executeSqlFromFile(conn, "sql_delete.sql");   
        worker.run();
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);

        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
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
        Helper.executeSqlFromFile(connDest, "importDest.sql");
        
        RunnerService runnerService = new RunnerService(sessionFactory);
        
        worker = new WorkerThread(runnerService.getRunner(1));
    }
}
