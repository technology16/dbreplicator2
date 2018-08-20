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


import static org.junit.Assert.assertEquals;

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
    private static final int REPLICATION_DELAY = 100;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("importIgnoreReplication.sql", "init_db/importRep2.sql", "init_db/importSource.sql", "init_db/importDest.sql");
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
        Helper.executeSqlFromFile(source, "sql_query/sql_insert_error.sql");
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        Helper.InfoSelect(source, "rep2_errors_log");
        List<MyTablesType> listSource = Helper.InfoTest(source, "t_table1");
        List<MyTablesType> listDest   = Helper.InfoTest(dest, "t_table1");
        assertEquals("Количество записей в источнике не верно!", 5, listSource.size());
        assertEquals("Количество записей в приемнике не верно!", 2, listDest.size());
    }
    
    /**
     * Инициализация раннеров
     */
    public static void initRunners() {
        RunnerService runnerService = new RunnerService(sessionFactory);

        worker = new WorkerThread(runnerService.getRunner(1));
    }
}
