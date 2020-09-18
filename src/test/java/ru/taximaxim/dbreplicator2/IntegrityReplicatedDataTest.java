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
import java.sql.PreparedStatement;
import java.sql.SQLException;

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
 * @author volodin_aa
 *
 */
public class IntegrityReplicatedDataTest extends AbstractReplicationTest {
    protected static final Logger LOG = Logger.getLogger(IntegrityReplicatedDataTest.class);

    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 100;

    protected static Runnable errorsIntegrityReplicatedData = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("importIntegrityReplicatedData.sql", "init_db/importRep2.sql", "init_db/importSource.sql", "init_db/importDest.sql");
        initRunners();
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        close();
    }

    /**
     * Проверка вставки
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testInsert() throws SQLException, IOException, InterruptedException {
        //Проверка вставки
        Helper.executeSqlFromFile(source, "sql_query/sql_insert.sql");
        worker.run();
        Helper.executeSqlFromFile(source, "sql_query/sql_update.sql");
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        verifyTable("t_table");
        verifyTable("t_table1");
        Helper.executeSqlFromFile(dest, "sql_query/sql_update.sql");
        Helper.executeSqlFromFile(dest, "sql_query/sql_delete.sql");
        Helper.InfoSelect(source, "rep2_errors_log");
        Helper.assertEmptyTable(source, "rep2_errors_log");

        errorsIntegrityReplicatedData.run();
        Thread.sleep(REPLICATION_DELAY);
        Helper.assertNotEmptyTable(source, "rep2_workpool_data");
        Helper.assertNotEmptyTable(source, "rep2_errors_log where c_status = 0");

        Helper.executeSqlFromFile(source, "sql_query/sql_delete.sql");
        worker.run();
        Helper.executeSqlFromFile(source, "sql_query/sql_update.sql");
        worker.run();
        Thread.sleep(REPLICATION_DELAY);

        PreparedStatement statement = source.prepareStatement("INSERT INTO T_TABLE1 (id, _int, _boolean, _long, _decimal, _double, _float, _string, _byte, _date, _time, _timestamp) select  id_foreign, 2, true, 5968326496, 99.65, 5.62, 79.6, 'rasfar', 0, now(), now(), now() from rep2_workpool_data where id_table='T_TABLE1' and c_operation = 'D'");
        statement.execute();
        statement.close();
        worker.run();
        Thread.sleep(REPLICATION_DELAY);

        errorsIntegrityReplicatedData.run();
        Thread.sleep(REPLICATION_DELAY);

        Helper.InfoSelect(source, "rep2_errors_log");
        Helper.assertEmptyTable(source, "rep2_errors_log where c_status = 0");
    }

    /**
     * Инициализация раннеров
     */
    public static void initRunners() {
        RunnerService runnerService = new RunnerService(sessionFactory);

        worker = new WorkerThread(runnerService.getRunner(1));
        errorsIntegrityReplicatedData = new WorkerThread(runnerService.getRunner(16));
    }
}
