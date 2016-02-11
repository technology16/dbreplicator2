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

import java.util.List;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractReplicationTest;
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
public class CronSchedulerTest extends AbstractReplicationTest {
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 20000;
    protected static final Logger LOG = Logger.getLogger(CronSchedulerTest.class);

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("importCronScheduler.sql", "init_db/importRep2.sql", "init_db/importSource.sql", "init_db/importDest.sql");
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
    
    @Test
    public void testTaskScheduler() throws Exception {
        //Проверка внешних ключей
        LOG.info("Проверка внешних ключей");
        Helper.executeSqlFromFile(conn, "sql_query/sql_insert.sql");

        // Запуск всех тасков
        Core.getCronPool().start();
        Thread.sleep(REPLICATION_DELAY);

        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table2");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table2");
        Helper.AssertEquals(listSource, listDest);

        listSource = Helper.InfoTest(conn, "t_table3");
        listDest   = Helper.InfoTest(connDest, "t_table3");
        Helper.AssertEquals(listSource, listDest);
    }
}
