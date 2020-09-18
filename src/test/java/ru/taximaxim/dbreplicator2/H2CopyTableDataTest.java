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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import ru.taximaxim.dbreplicator2.model.StrategyModel;
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
public class H2CopyTableDataTest extends AbstractReplicationTest {
    protected static final Logger LOG = Logger.getLogger(H2CopyTableDataTest.class);
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 100;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("import.sql", "init_db/importRep2.sql",
                "init_db/importSource.sql", "init_db/importDest.sql");
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
    }

    /**
     * Проверка Супер логов
     *
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testRep2TablesClearing() throws SQLException, InterruptedException {
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        Helper.assertEmptyTable(source, "rep2_superlog");
        Helper.assertEmptyTable(source, "rep2_workpool_data");

        List<MyTablesType> listSource = Helper.InfoTest(source, "t_table");
        List<MyTablesType> listDest = Helper.InfoTest(dest, "t_table");
        Helper.AssertEquals(listSource, listDest);

        LOG.info("<======Inception======>");
        Helper.InfoList(listSource);
        LOG.info("=======Inception=======");
        Helper.InfoList(listDest);
        LOG.info(">======Inception======<");

        Helper.assertEmptyTable(source, "rep2_superlog");
    }

    /**
     * Тестирование null значений в разных типах
     *
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testNull() throws SQLException, IOException, InterruptedException {
        // Проверка null
        LOG.info("Проверка null");
        Helper.executeSqlFromFile(source, "sql_query/sql_null.sql");
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        Helper.InfoSelect(source, "rep2_errors_log");
        List<MyTablesType> listSource = Helper.InfoTest(source, "t_table4");
        List<MyTablesType> listDest = Helper.InfoTest(dest, "t_table4");
        Helper.AssertEqualsNull(listSource, listDest);

        listSource = Helper.InfoTest(source, "t_table5");
        listDest = Helper.InfoTest(dest, "t_table5");
        Helper.AssertEqualsNull(listSource, listDest);

        Helper.InfoNull(source, "t_table4", 1);
        Helper.InfoNull(source, "t_table5", 2);

        Helper.InfoNull(dest, "t_table4", 1);
        Helper.InfoNull(dest, "t_table5", 2);

        Helper.assertEmptyTable(source, "rep2_superlog");
    }

    @Test
    public void controlSumm() throws SQLException, InterruptedException {
        Integer i = 1;
        String s = "tab";
        Long l = 5L;

        controlSumm(i, s, l);
        controlSumm(i, null, l);
        controlSumm(i, null, null);
        controlSumm(i, s, null);
        controlSumm(null, s, null);
        controlSumm(null, s, l);
        controlSumm(null, null, l);
        controlSumm(null, null, null);
        Helper.InfoSelect(source, "rep2_errors_log");

        try (Statement stat = source.createStatement()) {
            stat.execute("delete from rep2_errors_log");
        }
    }

    private void controlSumm(Integer idRunner, String tableName, Long idForeign)
            throws SQLException {
        errorsLog.add(idRunner, tableName, idForeign, "Error");
        errorsLog.setStatus(idRunner, tableName, idForeign, 1);

        String textRunner = idRunner == null ? "is null" : "= " + idRunner.toString();
        String textTable = tableName == null ? "is null" : "= '" + tableName + "'";
        String textForeign = idForeign == null ? "is null" : "= " + idForeign.toString();

        String query = "select * from rep2_errors_log where id_runner %s and id_table %s and id_foreign %s and c_status = 1";

        try (Statement stat = source.createStatement();
                ResultSet rs = stat.executeQuery(String.format(query, textRunner, textTable, textForeign))) {
            if (!rs.next()) {
                fail(String.format("нет записи id_runner %s and id_table %s and id_foreign %s",
                        textRunner, textTable, textForeign));
            }

            assertEquals("Неверный результат id_runner", idRunner, rs.getObject("id_runner"));
            assertEquals("Неверный результат id_table", tableName, rs.getObject("id_table"));
            assertEquals("Неверный результат id_foreign", idForeign, rs.getObject("id_foreign"));
        }
    }

    /**
     * Проверка внешних ключей вставка в главную таблицу вставка таблицу
     * подчиненную изменение главной таблицы
     *
     * репликация
     *
     * вставка таблицу подчиненную изменение главной таблицы
     *
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testForeignKey() throws SQLException, IOException, InterruptedException {
        // Проверка внешних ключей
        LOG.info("Проверка внешних ключей");
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 50);
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 51);
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 52);
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 53);
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 54);
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 55);
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 56);
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 57);
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 58);
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 59);
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 60);
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 61);
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 62);
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key.sql", 63);
        worker.run();
        Thread.sleep(REPLICATION_DELAY);

        // Данные должны реплицироваться за 1 проход, т.к. в случае наличия
        // ошибок стратегия начнет их заново реплицировать

        verifyTable("t_table2");
        verifyTable("t_table3");
    }

    /**
     * Проверка вставки
     *
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testInsert() throws SQLException, IOException, InterruptedException {
        // Проверка вставки
        Helper.executeSqlFromFile(source, "sql_query/sql_insert.sql");
        testWorker();
    }

    /**
     * Проверка обновления
     *
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testUpdate() throws SQLException, IOException, InterruptedException {
        testInsert();
        // Проверка обновления
        LOG.info("Проверка обновления");
        Helper.executeSqlFromFile(source, "sql_query/sql_update.sql");
        testWorker();
    }

    /**
     * Проверка удаления
     *
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testDelete() throws SQLException, IOException, InterruptedException {
        testInsert();
        LOG.info("Проверка удаления");
        // Проверка удаления
        Helper.executeSqlFromFile(source, "sql_query/sql_delete.sql");
    }

    /**
     * Проверка вставки и обновления
     *
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testInsertUpdate() throws SQLException, IOException, InterruptedException {
        // Проверка вставки и обновления
        LOG.info("Проверка вставки и обновления");
        Helper.executeSqlFromFile(source, "sql_query/sql_insert.sql");
        Helper.executeSqlFromFile(source, "sql_query/sql_update.sql");
        testWorker();
    }

    /**
     * Проверка вставки и удаления
     *
     * @throws SQLException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testInsertDelete() throws SQLException, IOException, InterruptedException {
        LOG.info("Проверка вставки и удаления");
        // Проверка вставки и удаления
        Helper.executeSqlFromFile(source, "sql_query/sql_insert.sql");
        Helper.executeSqlFromFile(source, "sql_query/sql_delete.sql");
        testWorker();
    }

    private void testWorker() throws InterruptedException, SQLException {
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        verifyTable("t_table");
        verifyTable("t_table1");
        Helper.assertEmptyTable(source, "rep2_superlog");
    }

    /**
     * Проверка чтения параметров стратегии
     */
    @Test
    public void paramTest() {
        RunnerService runnerService = new RunnerService(sessionFactory);
        List<StrategyModel> strategyModels = runnerService.getRunner(1)
                .getStrategyModels();
        boolean hasStrategy = false;
        for (StrategyModel strategyModel : strategyModels) {
            if (strategyModel.getId() == 1) {
                assertEquals("Ошибка в параметре key1 стратегии 1!",
                        "value1", strategyModel.getParam("key1"));
                assertEquals("Ошибка в параметре key2 стратегии 1!",
                        "'value2'", strategyModel.getParam("key2"));
                hasStrategy = true;
            }
        }
        assertTrue("В раннере 1 отсутствует стратегия 1!", hasStrategy);
    }

    protected void checkErrorInRunner(int runnerId) throws SQLException {
        // Проверяем запись об ошибке в логе
        try (PreparedStatement selectError = source.prepareStatement(
                "SELECT * FROM rep2_errors_log WHERE id_runner=" + runnerId);
                ResultSet errorResult = selectError.executeQuery();) {
            assertTrue("В логе ошибок отсутствует запись об ошибке " + runnerId
                    + " раннера!", errorResult.next());
            LOG.info(Jdbc.resultSetToString(errorResult,
                    new ArrayList<>(JdbcMetadata.getColumns(errorResult))));
        }
    }

    /**
     * Проверка перехвата неожиданного завершения потока задачи
     *
     * @throws InterruptedException
     * @throws SQLException
     */
    @Test
    public void exceptionInTaskTest() throws InterruptedException, SQLException {
        // Запускаем задачи
        RunnerService runnerService = new RunnerService(sessionFactory);

        Core.getThreadPool().start(runnerService.getRunner(16));
        Thread.sleep(REPLICATION_DELAY);

        checkErrorInRunner(16);
    }

    /**
     * Проверка перехвата неожиданного завершения потока пула потоков
     *
     * @throws InterruptedException
     * @throws SQLException
     */
    @Test
    public void exceptionInThreadPoolTest() throws InterruptedException, SQLException {
        // Запускаем задачи
        RunnerService runnerService = new RunnerService(sessionFactory);

        Core.getThreadPool().start(runnerService.getRunner(17));
        Thread.sleep(REPLICATION_DELAY);

        checkErrorInRunner(17);
    }
}
