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

import static org.junit.Assert.*;

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
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.strategies.errors.ErrorsLog;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Тест репликации данных между базами H2-H2. 
 * 
 * Данный тест использует асинхронный менеджер записей супер лог таблицы, 
 * поэтому после каждого цикла репликации вызывается инструкция 
 * Thread.sleep(REPLICATION_DELAY); Тест может некорректно работать на медленных 
 * машинах, при необходимости подгонять величину задержки вручную!
 * 
 * @author volodin_aa
 *
 */
public class H2CopyTableDataTest {
    protected static final Logger LOG = Logger.getLogger(H2CopyTableDataTest.class);
    
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 1500;
    
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
     * Проверка Супер логов
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException 
     */
    @Test
    public void testRep2TablesClearing() throws SQLException, ClassNotFoundException, IOException, InterruptedException {

        Helper.executeSqlFromFile(conn, "importSourceData.sql");
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        
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
     * @throws InterruptedException 
     */
    @Test
    public void testNull() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        //Проверка null
        LOG.info("Проверка null");
        Helper.executeSqlFromFile(conn, "sql_null.sql");   
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table4");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table4");
        
        Helper.AssertEqualsNull(listSource, listDest);
//
        listSource = Helper.InfoTest(conn, "t_table5");
        listDest   = Helper.InfoTest(connDest, "t_table5");
        Helper.AssertEqualsNull(listSource, listDest);
        
        Helper.InfoNull(conn, "t_table4", 1);
        Helper.InfoNull(conn, "t_table5", 2);

        Helper.InfoNull(connDest, "t_table4", 1);
        Helper.InfoNull(connDest, "t_table5", 2);
    }
    
    @Test
    public void controlSumm() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        ErrorsLog errorLog = new ErrorsLog(conn);
        String SQL_UPDATE = "UPDATE rep2_errors_log SET c_status = ? where ";
        Integer i = 1;
        String s = "tab";
        Long l = (long) 5;
        
        int chechSumm = errorLog.getCheckSum (i, s, l);
        String sql = errorLog.getNullSql (i, s, l);
        LOG.info(String.format("chechSumm: [%s] sql: [%s %s]", chechSumm, SQL_UPDATE, sql));
        
        chechSumm = errorLog.getCheckSum (null, s, l);
        sql = errorLog.getNullSql (null, s, l);
        LOG.info(String.format("chechSumm: [%s] sql: [%s %s]", chechSumm, SQL_UPDATE, sql));
        
        chechSumm = errorLog.getCheckSum (i, null, l);
        sql = errorLog.getNullSql (i, null, l);
        LOG.info(String.format("chechSumm: [%s] sql: [%s %s]", chechSumm, SQL_UPDATE, sql));
        
        chechSumm = errorLog.getCheckSum (i, s, null);
        sql = errorLog.getNullSql (i, s, null);
        LOG.info(String.format("chechSumm: [%s] sql: [%s %s]", chechSumm, SQL_UPDATE, sql));
        
        chechSumm = errorLog.getCheckSum (null, null, l);
        sql = errorLog.getNullSql (null, null, l);
        LOG.info(String.format("chechSumm: [%s] sql: [%s %s]", chechSumm, SQL_UPDATE, sql));
        
        chechSumm = errorLog.getCheckSum (null, s, null);
        sql = errorLog.getNullSql (null, s, null);
        LOG.info(String.format("chechSumm: [%s] sql: [%s %s]", chechSumm, SQL_UPDATE, sql));
        
        chechSumm = errorLog.getCheckSum (i, null, null);
        sql = errorLog.getNullSql (i, null, null);
        LOG.info(String.format("chechSumm: [%s] sql: [%s %s]", chechSumm, SQL_UPDATE, sql));
        
        chechSumm = errorLog.getCheckSum (null, null, null);
        sql = errorLog.getNullSql (null, null, null);
        LOG.info(String.format("chechSumm: [%s] sql: [%s %s]", chechSumm, SQL_UPDATE, sql));
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
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        worker.run();
        Helper.InfoSelect(conn, "rep2_errors_log");
        Thread.sleep(REPLICATION_DELAY);
        errorsCountWatchdogWorker.run();
        Thread.sleep(REPLICATION_DELAY);
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table2");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table2");
        Helper.AssertEquals(listSource, listDest);

        listSource = Helper.InfoTest(conn, "t_table3");
        listDest   = Helper.InfoTest(connDest, "t_table3");
        Helper.AssertEquals(listSource, listDest);
        Helper.InfoSelect(conn, "rep2_errors_log");
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
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
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
     * @throws InterruptedException 
     */
    @Test
    public void testUpdate() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        testInsert();
        //Проверка обновления
        LOG.info("Проверка обновления");
        Helper.executeSqlFromFile(conn, "sql_update.sql");   
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
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
     * @throws InterruptedException 
     */
    @Test
    public void testDelete() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        testInsert();
        LOG.info("Проверка удаления");
        //Проверка удаления
        Helper.executeSqlFromFile(conn, "sql_delete.sql");   
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
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
     * @throws InterruptedException 
     */
    @Test
    public void testInsertUpdate() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
      //Проверка вставки и обновления
        LOG.info("Проверка вставки и обновления");
        Helper.executeSqlFromFile(conn, "sql_insert.sql");   
        Helper.executeSqlFromFile(conn, "sql_update.sql");   
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
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
     * @throws InterruptedException 
     */
    @Test
    public void testInsertDelete() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        LOG.info("Проверка вставки и удаления");
      //Проверка вставки и удаления
        Helper.executeSqlFromFile(conn, "sql_insert.sql");   
        Helper.executeSqlFromFile(conn, "sql_delete.sql");   
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);

        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
    }
    
    /**
     * Проверка чтения параметров стратегии 
     */
    @Test
    public void paramTest(){
        RunnerService runnerService = new RunnerService(sessionFactory);
        List<StrategyModel> strategyModels = runnerService.getRunner(1).getStrategyModels();
        boolean hasStrategy = false;
        for (StrategyModel strategyModel: strategyModels) {
            if (strategyModel.getId()==1) {
                assertTrue("Ошибка в параметре key1 стратегии 1!", strategyModel.getParam("key1").equals("value1"));
                assertTrue("Ошибка в параметре key2 стратегии 1!", strategyModel.getParam("key2").equals("'value2'"));
                hasStrategy = true;
            }
        }
        assertTrue("В раннере 1 отсутствует стратегия 1!", hasStrategy);
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
        errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(7));
    }
}
