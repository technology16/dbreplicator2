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
import java.util.ArrayList;
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
 * Данный тест использует асинхронный менеджер записей супер лог таблицы,
 * поэтому после каждого цикла репликации вызывается инструкция
 * Thread.sleep(REPLICATION_DELAY); Тест может некорректно работать на медленных
 * машинах, при необходимости подгонять величину задержки вручную!
 *
 * @author galiev_mr
 *
 */
public class DisableReplicationTest extends AbstractReplicationTest {

    protected static final Logger LOG = Logger.getLogger(DisableReplicationTest.class);

    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 100;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("importDisableReplication.sql", "init_db/importRep2.sql",
                "init_db/importSource.sql", "init_db/importDest.sql");
        initWorkers();
    }

    private final static List<WorkerThread> WORKERS = new ArrayList<>();

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        close();
    }

    /**
     * Проверка дополнительного параметра is_enabled
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testDisabledReplication() throws SQLException, IOException, InterruptedException {
        // вставка данных
        Helper.executeSqlFromFile(source, "sql_query/sql_insert.sql");
        runWorkers();
        Thread.sleep(REPLICATION_DELAY);

        compareResult("T_TABLE",  true);  // standard to enabled
        compareResult("T_TABLE1", false); // standard to disabled
        compareResult("T_TABLE2", true);  // enabled to standard
        compareResult("T_TABLE3", true);  // enabled to enabled
        compareResult("T_TABLE4", false); // enabled to disabled
        compareResult("T_TABLE5", false); // disabled to standard
        compareResult("T_TABLE6", false); // disabled to enabled
        compareResult("T_TABLE7", false); // disabled to disabled
        compareResult("T_TABLE8", false); // broken to standard
    }

    private void compareResult(String tableName, boolean expectEquals)
            throws InterruptedException, SQLException {

        List<MyTablesType> listDest = Helper.InfoTest(dest, tableName);
        if (expectEquals) {
            List<MyTablesType> listSource = Helper.InfoTest(source, tableName);
            Helper.AssertEquals(listSource, listDest);
        } else {
            assertTrue("Таблица должна быть пустой", listDest.isEmpty());
        }
    }

    private static void initWorkers() {
        RunnerService runnerService = new RunnerService(sessionFactory);
        WORKERS.add(new WorkerThread(runnerService.getRunner(1)));
        WORKERS.add(new WorkerThread(runnerService.getRunner(2)));
        WORKERS.add(new WorkerThread(runnerService.getRunner(3)));
    }

    private static void runWorkers() {
        for (WorkerThread worker : WORKERS) {
            worker.run();
        }
    }
}
