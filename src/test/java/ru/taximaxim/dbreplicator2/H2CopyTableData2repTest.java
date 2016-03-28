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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractReplicationTest;
import ru.taximaxim.dbreplicator2.jdbc.Jdbc;
import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.model.RunnerService;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;

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
public class H2CopyTableData2repTest extends AbstractReplicationTest {
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 400;
    
    protected static final Logger LOG = Logger.getLogger(H2CopyTableDataTest.class);
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp(null, null, "init_db/importRep2.sql", "init_db/importSource.sql", "init_db/importDest.sql"); 
        initRunners();
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        close();
    }
    
    /**
     * Инициализация раннеров
     */
    public static void initRunners() {
        RunnerService runnerService = new RunnerService(sessionFactory);

        worker = new WorkerThread(runnerService.getRunner(1));
        worker2 = new WorkerThread(runnerService.getRunner(2));
        errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(6));
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
     * @throws Exception 
     */
    @Test
    public void testForeignKey() throws Exception {
        Thread.sleep(REPLICATION_DELAY);
        //Проверка внешних ключей
        LOG.info("Проверка внешних ключей");
        Helper.executeSqlFromFile(conn, "sql_query/sql_foreign_key.sql", 20);
        
        workerRun();
        Helper.executeSqlFromFile(connDest,  "sql_query/sql_foreign_key2.sql", 20);
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
     * @throws Exception 
     */
    @Test
    public void testUpdate() throws Exception {
        testInsert();
        //Проверка обновления
        LOG.info("Проверка обновления");
        Helper.executeSqlFromFile(conn, "sql_query/sql_update.sql");
        workerRun();
        Helper.executeSqlFromFile(connDest, "sql_query/sql_update2.sql");
        workerRun2();
        
        workerRun();
        workerRun2();
        
        verifyTables();
        
        workerEnd();
        workerEnd2();
        
        int count = Helper.InfoCount(conn,  "rep2_superlog");
        assertTrue(String.format("Количество записей должно быть пустым [%s == 0]", count), 0 == count);
    }
    
    /**
     * Проверка удаления
     * @throws Exception 
     */
    @Test
    public void testDelete() throws Exception {
        testInsert();
        LOG.info("Проверка удаления");
        //Проверка удаления
        Helper.executeSqlFromFile(conn, "sql_query/sql_delete.sql");
        workerRun();
        Helper.executeSqlFromFile(connDest, "sql_query/sql_delete.sql");
        workerRun2();
        
        workerRun();
        workerRun2();
        
        verifyTables();
        
        workerEnd();
        workerEnd2();
        
        int count = Helper.InfoCount(conn,  "rep2_superlog");
        assertTrue(String.format("Количество записей должно быть пустым [%s == 0]", count), 0 == count);
    }
    
    /**
     * Проверка вставки и обновления
     * @throws Exception 
     */
    @Test
    public void testInsertUpdate() throws Exception {
      //Проверка вставки и обновления
        LOG.info("Проверка вставки и обновления");
        Helper.executeSqlFromFile(conn, "sql_query/sql_insert.sql");
        Helper.executeSqlFromFile(conn, "sql_query/sql_update.sql");
        workerRun();
        Helper.executeSqlFromFile(connDest, "sql_query/sql_update2.sql");
        workerRun2();
        
        workerRun();
        workerRun2();
        
        verifyTables();
        
        workerEnd();
        workerEnd2();
        
        int count = Helper.InfoCount(conn,  "rep2_superlog");
        assertTrue(String.format("Количество записей должно быть пустым [%s == 0]", count), 0 == count);
    }
    
    /**
     * Проверка вставки и удаления
     * @throws Exception 
     */
    @Test
    public void testInsertDelete() throws Exception {
        LOG.info("Проверка вставки и удаления");
      //Проверка вставки и удаления
        Helper.executeSqlFromFile(conn, "sql_query/sql_insert.sql");
        Helper.executeSqlFromFile(conn, "sql_query/sql_delete.sql");
        workerRun();
        Helper.executeSqlFromFile(connDest, "sql_query/sql_insert.sql");
        Helper.executeSqlFromFile(connDest, "sql_query/sql_delete.sql");
        workerRun2();
        
        workerRun();
        workerRun2();
        
        verifyTables();
        
        workerEnd();
        workerEnd2();
        
        int count = Helper.InfoCount(conn,  "rep2_superlog");
        assertTrue(String.format("Количество записей должно быть пустым [%s == 0]", count), 0 == count);
    }

    /**
     * Проверка вставки 
     * @throws Exception 
     */
    @Test
    public void testInsert() throws Exception {
      //Проверка вставки
        Helper.executeSqlFromFile(conn, "sql_query/sql_insert.sql");
        workerRun();
        workerRun2();

        workerRun();
        workerRun2();
        
        verifyTables();
        
        workerEnd();
        workerEnd2();
        int count = Helper.InfoCount(conn,  "rep2_superlog");
        assertTrue(String.format("Количество записей должно быть пустым [%s == 0]", count), 0 == count);
    }
    
    protected void verifyTables() throws SQLException, InterruptedException {
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
    }
    
    public void workerRun() throws Exception{
        worker.run();
        Helper.executeSqlFromSql(conn, "UPDATE T_TAB SET _value = ?", "dest");
        Thread.sleep(REPLICATION_DELAY);
    }
    
    public void workerRun2() throws Exception{
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
