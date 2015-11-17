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
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractReplicationTest;
import ru.taximaxim.dbreplicator2.model.RunnerService;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;
/**
 * Тест репликации данных между базами H2-H2. 
 * 
 * Данный тест будет приводить к бесконечному циклу
 * в случае ошибки в алгоритме включения ошибок репликации в репликацию,
 * если последняя выборка меньше fetchsize
 * (Придется принудительно завершать процесс)
 * 
 * @author petrov_im
 *
 */
public class ErrorReplicationTest extends AbstractReplicationTest{
    protected static final Logger LOG = Logger.getLogger(ErrorReplicationTest.class);
    
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 1500;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("src/test/resources/hibernateIgnoreReplication.cfg.xml", "importRep2.sql", "importSource.sql", "importDest.sql");
        initRunners();
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        close();
    }
    
    /**
     * В случае появления нереплицируемой ошибки и нарушения
     * алгоритма выборки из воркпула (если выборка меньше fetchsize)
     * гет приводит к бесконечному циклу.
     * (Придется завершать процесс)
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testErroReplicated() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
      //Проверка вставки
        Helper.executeSqlFromFile(conn, "sql_insert_error.sql");
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table1");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table1");
        assertTrue(String.format("Количество записей не равны [%s != 5]", listSource.size()),
                listSource.size() == 5);
        assertTrue(String.format("Количество записей не равны [%s != 2]", listDest.size()),
                listDest.size() == 2);

    }
    
    /**
     * Инициализация раннеров
     */
    public static void initRunners() {
        RunnerService runnerService = new RunnerService(sessionFactory);

        worker = new WorkerThread(runnerService.getRunner(1));
        errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(7));
    }
}
