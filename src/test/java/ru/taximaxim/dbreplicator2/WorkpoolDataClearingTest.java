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
import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.model.RunnerService;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Тест очистки rep2_workpool_data в случае если есть записи об операциях над 
 * несуществующими записями.
 * 
 * Тест расчитан на настройки стратегии репликации  batchSize=1 и fetchSize=1 
 * 
 * @author volodin_aa
 *
 */
public class WorkpoolDataClearingTest {
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 500;
    protected static final Logger LOG = Logger.getLogger(WorkpoolDataClearingTest.class);
    protected static SessionFactory sessionFactory;
    protected static Session session;
    protected static ConnectionFactory connectionFactory;
    protected static Connection conn = null;
    protected static Connection connDest = null;
    protected static Runnable workerPg = null;
    protected static Runnable workerMs = null;
    protected static Runnable errorsCountWatchdogWorker = null;

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Core.getConfiguration("src/test/resources/hibernateWorkpoolDataClearing.cfg.xml");
        sessionFactory = Core.getSessionFactory();
        session = sessionFactory.openSession();
        connectionFactory = Core.getConnectionFactory();
        initialization();
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if(conn!=null)
            conn.close();
        if(connDest!=null)
            connDest.close();
        if(session!=null)
            session.close();
        Core.connectionFactoryClose();
        Core.sessionFactoryClose();
        Core.statsServiceClose();
        Core.configurationClose();
    }

    /**
     * Инициализация
     */
    public static void initialization() throws ClassNotFoundException, SQLException, IOException{
        LOG.info("initialization");
        String source = "source";
        conn = connectionFactory.getConnection(source);
        
        Helper.executeSqlFromFile(conn, "importRep2.sql");
        Helper.executeSqlFromFile(conn, "importSourceOffset.sql");
        
        String dest = "dest";
        connDest = connectionFactory.getConnection(dest);
        Helper.executeSqlFromFile(connDest, "importRep2.sql");
        Helper.executeSqlFromFile(connDest, "importDest.sql");
        
        RunnerService runnerService = new RunnerService(sessionFactory);

        workerPg = new WorkerThread(runnerService.getRunner(1));
        errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(7));
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
    public void testWorkpoolDataClearing() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        // Добавляем операцию над несуществующеми данными
        try (PreparedStatement statement = 
                conn.prepareStatement("INSERT INTO REP2_SUPERLOG (id_foreign, id_table, c_operation, c_date, id_transaction, id_pool) VALUES (?, 'T_TABLE2', ?, now(), 0, 'source')")) {
            statement.setInt(1, 1234567890);
            statement.setString(2, "U");
            statement.executeUpdate();
            
            statement.setInt(1, 987654321);
            statement.setString(2, "I");
            statement.executeUpdate();
        }
        
        //Проверка внешних ключей
        LOG.info("Проверка внешних ключей");
        Helper.executeSqlFromFile(conn, "sql_foreign_key_error.sql");
        
        workerPg.run();
        Thread.sleep(REPLICATION_DELAY);
        workerPg.run();
        Thread.sleep(REPLICATION_DELAY);
        workerPg.run();
        Thread.sleep(REPLICATION_DELAY);
        
        // Выводим данные из rep2_superlog_table
        try (PreparedStatement select = 
                conn.prepareStatement("SELECT * FROM REP2_WORKPOOL_DATA");
                ResultSet result = select.executeQuery();
        ) {
            List<String> cols = 
                    new ArrayList<String>(JdbcMetadata.getColumns(conn, "REP2_WORKPOOL_DATA"));
            
            while (result.next()) {
                LOG.info("================================");
                for (String col : cols) {
                    LOG.info(col + "=" + result.getString(col));
                }
                LOG.info("================================");
            }
        }
        
       // errorsCountWatchdogWorker.run();
        workerPg.run();
        Thread.sleep(REPLICATION_DELAY);
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table2");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table2");
        Helper.AssertEquals(listSource, listDest);

        listSource = Helper.InfoTest(conn, "t_table3");
        listDest   = Helper.InfoTest(connDest, "t_table3");
        assertTrue(String.format("Количество записей [%s == 8 и %s = 2]", listSource.size(), listDest.size()),
                listSource.size() == 8 && listDest.size() == 2);
    }    
}
