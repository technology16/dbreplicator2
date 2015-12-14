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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractReplicationTest;
import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.model.RunnerService;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;

/**
 * @author mardanov_rm
 *
 */
public class OffsetTest extends AbstractReplicationTest {
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 500;
    protected static final Logger LOG = Logger.getLogger(OffsetTest.class);

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("importOffset.sql", "init_db/importRep2.sql", "init_db/importSourceOffset.sql", "init_db/importDest.sql");
        initRunners();
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        close();
    }
    
    /**
     * Инициализация раннеров
     */
    public static void initRunners() {
        RunnerService runnerService = new RunnerService(sessionFactory);

        worker = new WorkerThread(runnerService.getRunner(1));
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
     * @throws Exception 
     */
    @Test
    public void testOffset() throws Exception {
        //Проверка внешних ключей
        LOG.info("Проверка внешних ключей");
        Helper.executeSqlFromFile(conn, "sql_query/sql_foreign_key_error.sql");
        
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        worker.run();
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
        
        //errorsCountWatchdogWorker.run();
        worker.run();
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
