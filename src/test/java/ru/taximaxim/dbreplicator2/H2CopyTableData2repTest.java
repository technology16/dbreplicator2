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
    private static final int REPLICATION_DELAY = 100;

    private static final Logger LOG = Logger.getLogger(H2CopyTableDataTest.class);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("import.sql", "init_db/importRep2.sql", "init_db/importSource.sql", "init_db/importDest.sql");
        initRunners();
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        close();
    }

    /**
     * Инициализация раннеров
     */
    private static void initRunners() {
        RunnerService runnerService = new RunnerService(sessionFactory);

        worker = new WorkerThread(runnerService.getRunner(1));
        worker2 = new WorkerThread(runnerService.getRunner(2));
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
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 20);

        workerRun();
        Helper.executeSqlFromFile(dest,  "sql_query/sql_foreign_key2.sql", 20);
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
                source.prepareStatement("SELECT * FROM rep2_workpool_data")) {
            ResultSet result = select.executeQuery();
            List<String> cols =
                    new ArrayList<>(JdbcMetadata.getColumns(source, "REP2_WORKPOOL_DATA"));
            while (result.next()) {
                LOG.info(Jdbc.resultSetToString(result, cols));
            }
        }

        workerRun();
        Thread.sleep(REPLICATION_DELAY);
        Thread.sleep(REPLICATION_DELAY);

        verifyTable("t_table2");
        verifyTable("t_table3");
        verifyTable("t_table2");
        verifyTable("t_table3");
        verifyTable("t_table4");
        verifyTable("t_table5");

        workerEnd();
        workerEnd2();

        Helper.assertEmptyTable(source, "rep2_superlog");
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
        Helper.executeSqlFromFile(source, "sql_query/sql_update.sql");
        workerRun();
        Helper.executeSqlFromFile(dest, "sql_query/sql_update2.sql");
        testWorkers();
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
        Helper.executeSqlFromFile(source, "sql_query/sql_delete.sql");
        workerRun();
        Helper.executeSqlFromFile(dest, "sql_query/sql_delete.sql");
        testWorkers();
    }

    /**
     * Проверка вставки и обновления
     * @throws Exception
     */
    @Test
    public void testInsertUpdate() throws Exception {
        //Проверка вставки и обновления
        LOG.info("Проверка вставки и обновления");
        Helper.executeSqlFromFile(source, "sql_query/sql_insert.sql");
        Helper.executeSqlFromFile(source, "sql_query/sql_update.sql");
        workerRun();
        Helper.executeSqlFromFile(dest, "sql_query/sql_update2.sql");
        testWorkers();
    }

    /**
     * Проверка вставки и удаления
     * @throws Exception
     */
    @Test
    public void testInsertDelete() throws Exception {
        LOG.info("Проверка вставки и удаления");
        //Проверка вставки и удаления
        Helper.executeSqlFromFile(source, "sql_query/sql_insert.sql");
        Helper.executeSqlFromFile(source, "sql_query/sql_delete.sql");
        workerRun();
        Helper.executeSqlFromFile(dest, "sql_query/sql_insert.sql");
        Helper.executeSqlFromFile(dest, "sql_query/sql_delete.sql");
        testWorkers();
    }

    /**
     * Проверка вставки
     * @throws Exception
     */
    @Test
    public void testInsert() throws Exception {
        //Проверка вставки
        Helper.executeSqlFromFile(source, "sql_query/sql_insert.sql");
        workerRun();
        testWorkers();
    }

    private void testWorkers() throws Exception {
        workerRun2();

        workerRun();
        workerRun2();

        verifyTables();

        workerEnd();
        workerEnd2();

        Helper.assertEmptyTable(source, "rep2_superlog");
    }

    private void verifyTables() throws SQLException, InterruptedException {
        verifyTable("t_table");
        verifyTable("t_table1");
        verifyTable("t_table2");
        verifyTable("t_table3");
        verifyTable("t_table4");
        verifyTable("t_table5");
    }

    private void workerRun() throws SQLException, InterruptedException {
        worker.run();
        Helper.executeSqlFromSql(source, "UPDATE T_TAB SET _value = ?", "dest");
        Thread.sleep(REPLICATION_DELAY);
    }

    private void workerRun2() throws SQLException, InterruptedException {
        worker2.run();
        Helper.executeSqlFromSql(dest, "UPDATE T_TAB SET _value = ?", "source");
        Thread.sleep(REPLICATION_DELAY);
    }

    private void workerEnd() throws SQLException, InterruptedException {
        Helper.executeSqlFromSql(source, "UPDATE T_TAB SET _value = ?", "");
    }

    private void workerEnd2() throws SQLException, InterruptedException {
        Helper.executeSqlFromSql(dest, "UPDATE T_TAB SET _value = ?", "");
    }
}
