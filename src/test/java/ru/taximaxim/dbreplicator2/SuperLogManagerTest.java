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

import static org.junit.Assert.*;

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
 * Тест очистки rep2_workpool_data в случае если есть записи об операциях над
 * несуществующими записями.
 * 
 * Тест расчитан на настройки стратегии репликации fetchSize=1
 * 
 * @author volodin_aa
 *
 */
public class SuperLogManagerTest extends AbstractReplicationTest {
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 100;
    protected static final Logger LOG = Logger.getLogger(SuperLogManagerTest.class);

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("importWorkpoolDataClearing.sql", "init_db/importRep2.sql",
                "init_db/importSourceOffset.sql", "init_db/importDest.sql");
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
    }

    /**
     * Проверка внешних ключей вставка в главную таблицу вставка таблицу
     * подчиненную изменение главной таблицы
     * 
     * репликация
     * 
     * вставка таблицу подчиненную изменение главной таблицы
     * 
     * @throws Exception
     */
    @Test
    public void testWorkpoolDataClearing() throws Exception {
        // Добавляем операцию над несуществующеми данными
        try (PreparedStatement statement = source.prepareStatement(
                "INSERT INTO REP2_SUPERLOG (id_foreign, id_table, c_operation, c_date, id_transaction, id_pool) VALUES (?, 'T_TABLE2', ?, now(), 0, 'source')")) {
            statement.setInt(1, 1234567890);
            statement.setString(2, "U");
            statement.executeUpdate();

            statement.setInt(1, 987654321);
            statement.setString(2, "I");
            statement.executeUpdate();
        }

        // Проверка внешних ключей
        LOG.info("Проверка внешних ключей");
        Helper.executeSqlFromFile(source, "sql_query/sql_foreign_key_error.sql");

        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        worker.run();
        Thread.sleep(REPLICATION_DELAY);

        // Выводим данные из rep2_superlog_table
        try (PreparedStatement select = source
                .prepareStatement("SELECT * FROM REP2_WORKPOOL_DATA");
                ResultSet result = select.executeQuery();) {
            List<String> cols = new ArrayList<String>(
                    JdbcMetadata.getColumns(source, "REP2_WORKPOOL_DATA"));

            while (result.next()) {
                LOG.info("================================");
                for (String col : cols) {
                    LOG.info(col + "=" + result.getString(col));
                }
                LOG.info("================================");
            }
        }

        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        verifyTable("t_table2");

        List<MyTablesType> listSource = Helper.InfoTest(source, "t_table3");
        List<MyTablesType> listDest = Helper.InfoTest(dest, "t_table3");
        listSource = Helper.InfoTest(source, "t_table3");
        listDest = Helper.InfoTest(dest, "t_table3");
        assertTrue(String.format("Количество записей [%s == 8 и %s = 2]",
                listSource.size(), listDest.size()),
                listSource.size() == 8 && listDest.size() == 2);
    }

    /**
     * Проверка корректности пропуска ошибок вставки в rep2_workpool_data
     *
     * @throws Exception
     */
    @Test
    public void testWorkPoolInsertError() throws Exception {
        // Вставляем данные в супер лог
        try (PreparedStatement statement = source.prepareStatement(
                "INSERT INTO REP2_SUPERLOG (id_superlog, id_foreign, id_table, c_operation, c_date, id_transaction, id_pool) "
                        + "VALUES (?, ?, ?, ?, now(), 0, 'source')")) {
            //  Таблица без настроек
            statement.setInt(1, 1);
            statement.setInt(2, 1234567890);
            statement.setString(3, "T_TABLE1");
            statement.setString(4, "U");
            statement.executeUpdate();

            // Ошибочная таблицы
            statement.setInt(1, 2);
            statement.setInt(2, 1234567890);
            statement.setString(3, "T_TABLE4");
            statement.setString(4, "U");
            statement.executeUpdate();

            // Обычные таблицы
            statement.setInt(1, 3);
            statement.setInt(2, 1234567890);
            statement.setString(3, "T_TABLE5");
            statement.setString(4, "U");
            statement.executeUpdate();

            statement.setInt(1, 4);
            statement.setInt(2, 1234567890);
            statement.setString(3, "T_TABLE6");
            statement.setString(4, "U");
            statement.executeUpdate();
        }

        // Вставляем задвоенные данные в ворк пул
        try (PreparedStatement statement = source.prepareStatement(
                "INSERT INTO REP2_WORKPOOL_DATA (id_superlog, id_runner, id_foreign, id_table, c_operation, c_date, id_transaction) "
                        + "VALUES (?, 3, ?, ?, ?, now(), 0)")) {
            statement.setInt(1, 2);
            statement.setInt(2, 1234567890);
            statement.setString(3, "T_TABLE4");
            statement.setString(4, "U");
            statement.executeUpdate();
        }

        // Запускаем супер лог менеджер
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        worker.run();
        Thread.sleep(REPLICATION_DELAY);

        // Проверяем что данные сохранились в супер логе
        LOG.info("================================");
        Helper.InfoSelect(source, "rep2_superlog");
        LOG.info("================================");
        Helper.InfoSelect(source, "rep2_workpool_data");
        LOG.info("================================");

        // Проверяем наличие записей в суперлоге
        try (PreparedStatement select = source.prepareStatement(
                "SELECT * "
                + "FROM REP2_SUPERLOG "
                + "WHERE id_superlog = ?")) {
            select.setInt(1, 1);
            try (ResultSet result = select.executeQuery()) {
                assertTrue("В REP2_SUPERLOG отсутсвует запись таблицы T_TABLE1 (без настроек)!", result.next());
            }
            
            select.setInt(1, 2);
            try (ResultSet result = select.executeQuery()) {
                assertTrue("В REP2_SUPERLOG отсутсвует запись таблицы T_TABLE4 (ошибочная)!", result.next());
            }
            
            select.setInt(1, 3);
            try (ResultSet result = select.executeQuery()) {
                assertTrue("В REP2_SUPERLOG отсутсвует запись таблицы T_TABLE5!", result.next());
            }
        }        
        // Проверяем наличие записей в ворк пуле
        try (PreparedStatement select = source.prepareStatement(
                "SELECT * "
                + "FROM REP2_WORKPOOL_DATA "
                + "WHERE id_superlog = ?")) {
            select.setInt(1, 4);
            try (ResultSet result = select.executeQuery()) {
                assertTrue("В REP2_WORKPOOL_DATA отсутсвует запись таблицы T_TABLE6!", result.next());
            }
        }        
    }
}
