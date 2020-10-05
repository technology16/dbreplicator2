package ru.taximaxim.dbreplicator2.strategies.errors;

import static org.junit.Assert.assertEquals;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import ru.taximaxim.dbreplicator2.abstracts.AbstractReplicationTest;
import ru.taximaxim.dbreplicator2.model.RunnerService;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;

@RunWith(MockitoJUnitRunner.class)
public class CountWatchgdogTest extends AbstractReplicationTest {


    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Mock
    private AppenderSkeleton appender;

    @Captor
    private ArgumentCaptor<LoggingEvent> logCaptor;

    private static RunnerService runnerService;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("errors/CountWatchgdogTest.sql", "init_db/importRep2.sql",
                "init_db/importSource.sql", "init_db/importDest.sql");

        runnerService = new RunnerService(sessionFactory);
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        close();
    }

    @Before
    public void addAppender() {
        Logger.getRootLogger().addAppender(appender);
    }

    @After
    public void removeAppender() {
        Logger.getRootLogger().removeAppender(appender);
    }

    /**
     * @throws SQLException
     */
    protected void clearErrorsLog() throws SQLException {
        try (Statement delete = error.createStatement()) {
            // Очищаем ошибки
            delete.executeUpdate("DELETE FROM rep2_errors_log");
        }
    }

    /**
     * Вставляем ошибки таблицы
     *
     * @param now
     * @throws SQLException
     */
    protected void insertActiveTableError(Integer runner, String table, String date,
            Long id) throws SQLException {
        // Добавляем активные ошибки
        try (PreparedStatement insert = error.prepareStatement(
                "INSERT INTO rep2_errors_log (id_runner, id_table, c_date, id_errors_log, c_error, c_status) values (?, ?, ?, ?, 'Error in table', 0)");) {
            insert.setObject(1, runner);
            insert.setObject(2, table);
            insert.setObject(3, date);
            insert.setObject(4, id);
            insert.executeUpdate();
        }
    }

    /**
     * Вставляем ошибки таблицы
     *
     * @param now
     * @throws SQLException
     */
    protected void insertActiveRunnerError(Integer runner, String date, Long id)
            throws SQLException {
        // Добавляем активные ошибки
        try (PreparedStatement insert = error.prepareStatement(
                "INSERT INTO rep2_errors_log (id_runner, c_date, id_errors_log, c_error, c_status) values (?, ?, ?, 'Error in runner', 0)");) {
            insert.setObject(1, runner);
            insert.setObject(2, date);
            insert.setObject(3, id);
            insert.executeUpdate();
        }
    }

    /**
     * Вставляем ошибки
     *
     * @param now
     * @throws SQLException
     */
    protected void insertActiveError(Integer runner, String table, String date, Long id)
            throws SQLException {
        // Добавляем активные ошибки
        try (PreparedStatement insert = error.prepareStatement(
                "INSERT INTO rep2_errors_log (id_runner, id_table, c_date, id_errors_log, id_foreign, c_error, c_status) values (?, ?, ?, ?, ?, 'Error in record', 0)");) {
            insert.setObject(1, runner);
            insert.setObject(2, table);
            insert.setObject(3, date);
            insert.setObject(4, id);
            insert.setObject(5, id);
            insert.executeUpdate();
        }
    }

    /**
     * Вставляем ошибки
     *
     * @param now
     * @throws SQLException
     */
    protected void insertActiveErrors(Integer runner, String table, String date,
            Long idStart, Long idEnd) throws SQLException {
        // Добавляем активные ошибки
        if (idStart == idEnd) {
            insertActiveError(runner, table, date, idStart);
        } else {
            try (PreparedStatement insert = error.prepareStatement(
                    "INSERT INTO rep2_errors_log (id_runner, id_table, c_date, id_errors_log, id_foreign, c_error, c_status) values (?, ?, ?, ?, ?, 'Error in record', 0)");) {

                insert.setObject(1, runner);
                insert.setObject(2, table);
                insert.setObject(3, date);

                for (long i = idStart; i <= idEnd; i++) {
                    insert.setLong(4, i);
                    insert.setLong(5, i);
                    insert.addBatch();
                }

                insert.executeBatch();
            }
        }
    }

    /**
     * Вставляем отработанные ошибки
     *
     * @param now
     * @throws SQLException
     */
    protected void insertFixedError(Integer runner, String table, String date, Long id)
            throws SQLException {
        // Добавляем активные ошибки
        try (PreparedStatement insert = error.prepareStatement(
                "INSERT INTO rep2_errors_log (id_runner, id_table, c_date, id_errors_log, id_foreign, c_error, c_status) values (?, ?, ?, ?, ?, 'Fixed error in record', 1)");) {
            insert.setObject(1, runner);
            insert.setObject(2, table);
            insert.setObject(3, date);
            insert.setObject(4, id);
            insert.setObject(5, id);
            insert.executeUpdate();
        }
    }

    /**
     * Вставляем отработанные ошибки таблицы
     *
     * @param now
     * @throws SQLException
     */
    protected void insertFixedTableError(Integer runner, String table, String date,
            Long id) throws SQLException {
        // Добавляем активные ошибки
        try (PreparedStatement insert = error.prepareStatement(
                "INSERT INTO rep2_errors_log (id_runner, id_table, c_date, id_errors_log, c_error, c_status) values (?, ?, ?, ?, 'Fixed error in table', 1)");) {
            insert.setObject(1, runner);
            insert.setObject(2, table);
            insert.setObject(3, date);
            insert.setObject(4, id);
            insert.executeUpdate();
        }
    }

    /**
     * Вставляем отработанные ошибки раннера
     *
     * @param now
     * @throws SQLException
     */
    protected void insertFixedRunnerError(Integer runner, String date, Long id)
            throws SQLException {
        // Добавляем активные ошибки
        try (PreparedStatement insert = error.prepareStatement(
                "INSERT INTO rep2_errors_log (id_runner, c_date, id_errors_log, c_error, c_status) values (?, ?, ?, 'Fixed error in runner', 1)");) {
            insert.setObject(1, runner);
            insert.setObject(2, date);
            insert.setObject(3, id);
            insert.executeUpdate();
        }
    }

    /**
     * Проверка наличия активных ошибок с дефолтными настроками
     *
     * @throws SQLException
     */
    @Test
    public void testDefaultActiveErrors() throws SQLException {
        clearErrorsLog();

        // Задаем в явном виде текущее время.
        String now = FORMATTER.format(new Timestamp(new Date().getTime()));

        // Добавляем исправленные ошибки
        clearErrorsLog();
        insertFixedError(1, "table", now, 1L);
        insertFixedTableError(2, "table", now, 2L);
        insertFixedRunnerError(3, now, 3L);

        insertActiveErrors(1, "table", now, 11L, 19L);
        insertActiveTableError(2, "table", now, 20L);
        insertActiveRunnerError(3, now, 21L);

        // Запускаем CountWatchgdog
        WorkerThread errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(1));
        errorsCountWatchdogWorker.run();

        Mockito.verify(appender).doAppend(logCaptor.capture());
        assertEquals("", logCaptor.getValue().getRenderedMessage(), "\n" + "\n"
                + "В error превышен лимит в 0 ошибок!\n" + "\n" + "Ошибка 1 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 11 ] [ col MAX_ID_ERRORS_LOG = 11 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Ошибка 2 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 12 ] [ col MAX_ID_ERRORS_LOG = 12 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Ошибка 3 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 13 ] [ col MAX_ID_ERRORS_LOG = 13 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Ошибка 4 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 14 ] [ col MAX_ID_ERRORS_LOG = 14 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Ошибка 5 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 15 ] [ col MAX_ID_ERRORS_LOG = 15 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Ошибка 6 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 16 ] [ col MAX_ID_ERRORS_LOG = 16 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Ошибка 7 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 17 ] [ col MAX_ID_ERRORS_LOG = 17 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Ошибка 8 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 18 ] [ col MAX_ID_ERRORS_LOG = 18 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Ошибка 9 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 19 ] [ col MAX_ID_ERRORS_LOG = 19 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Ошибка 10 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 2 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = null ] [ col MAX_ID_ERRORS_LOG = 20 ] [ col COUNT = 1 ] [ col C_ERROR = Error in table ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Всего 11 ошибочных записей. Полный список ошибок доступен в таблице REP2_ERRORS_LOG.");
    }

    /**
     * Проверка при отсутствии активных ошибок
     *
     * @throws SQLException
     */
    @Test
    public void testFixedErrors() throws SQLException {

        // Задаем в явном виде текущее время.
        String now = FORMATTER.format(new Timestamp(new Date().getTime()));

        // Добавляем исправленные ошибки
        clearErrorsLog();
        insertFixedError(1, "table", now, 1L);
        insertFixedTableError(2, "table", now, 2L);
        insertFixedRunnerError(3, now, 3L);

        Mockito.reset(appender);

        // Запускаем CountWatchgdog
        WorkerThread errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(1));
        errorsCountWatchdogWorker.run();

        // проверяем что не было сообщений об ошибках
        Mockito.inOrder(appender).verifyNoMoreInteractions();
    }

    /**
     * Проверка наличия активных ошибок с настроками количества отображаемых
     * ошибок
     *
     * @throws SQLException
     */
    @Test
    public void testPartEmailActiveErrors() throws SQLException {
        clearErrorsLog();

        // Задаем в явном виде текущее время.
        String now = FORMATTER.format(new Timestamp(new Date().getTime()));

        clearErrorsLog();
        insertActiveErrors(1, "table", now, 1L, 9L);
        insertActiveTableError(2, "table", now, 10L);
        insertActiveRunnerError(3, now, 11L);

        // Запускаем CountWatchgdog
        WorkerThread errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(2));
        errorsCountWatchdogWorker.run();

        Mockito.verify(appender).doAppend(logCaptor.capture());
        assertEquals("", logCaptor.getValue().getRenderedMessage(), "\n" + "\n" + "В error превышен лимит в 0 ошибок!\n"
                + "\n" + "Ошибка 1 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 1 ] [ col MAX_ID_ERRORS_LOG = 1 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Ошибка 2 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 2 ] [ col MAX_ID_ERRORS_LOG = 2 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Ошибка 3 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 3 ] [ col MAX_ID_ERRORS_LOG = 3 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Ошибка 4 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 4 ] [ col MAX_ID_ERRORS_LOG = 4 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Ошибка 5 из 11 \n"
                + "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 5 ] [ col MAX_ID_ERRORS_LOG = 5 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now + " ]  ] ]\n" + "==========================================\n"
                + "Всего 11 ошибочных записей. Полный список ошибок доступен в таблице REP2_ERRORS_LOG.");
    }

    /**
     * Проверка наличия активных ошибок старше 5 секунды
     *
     * @throws InterruptedException
     * @throws SQLException
     */
    @Test
    public void testWhereActiveErrors() throws InterruptedException, SQLException {
        clearErrorsLog();

        // Добавляем ошибки старше 10 секунд
        String now10 = FORMATTER.format(new Timestamp(System.currentTimeMillis() - 10000));
        insertActiveError(1, "table", now10, 1L);
        insertActiveTableError(2, "table", now10, 2L);
        insertActiveRunnerError(3, now10, 3L);

        // Добавляем ошибки старше 3 секунд
        String now3 = FORMATTER.format(new Timestamp(System.currentTimeMillis() - 3000));
        insertActiveError(1, "table", now3, 4L);
        insertActiveTableError(2, "table", now3, 5L);
        insertActiveRunnerError(3, now3, 6L);

        // Запускаем CountWatchgdog
        WorkerThread errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(3));
        errorsCountWatchdogWorker.run();

        Mockito.verify(appender).doAppend(logCaptor.capture());
        assertEquals("", logCaptor.getValue().getRenderedMessage(), "\n" +
                "\n" +
                "В error превышен лимит в 0 ошибок!\n" +
                "\n" +
                "Ошибка 1 из 3 \n" +
                "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 1 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = 1 ] [ col MAX_ID_ERRORS_LOG = 1 ] [ col COUNT = 1 ] [ col C_ERROR = Error in record ] [ col C_DATE = "
                + now10 + " ]  ] ]\n" +
                "==========================================\n" +
                "Ошибка 2 из 3 \n" +
                "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 2 ] [ col ID_TABLE = table ] [ col ID_FOREIGN = null ] [ col MAX_ID_ERRORS_LOG = 2 ] [ col COUNT = 1 ] [ col C_ERROR = Error in table ] [ col C_DATE = "
                + now10 + " ]  ] ]\n" +
                "==========================================\n" +
                "Ошибка 3 из 3 \n" +
                "[ tableName = REP2_ERRORS_LOG [ row = [ col ID_RUNNER = 3 ] [ col ID_TABLE = null ] [ col ID_FOREIGN = null ] [ col MAX_ID_ERRORS_LOG = 3 ] [ col COUNT = 1 ] [ col C_ERROR = Error in runner ] [ col C_DATE = "
                + now10 + " ]  ] ]\n" +
                "==========================================\n" +
                "Всего 3 ошибочных записей. Полный список ошибок доступен в таблице REP2_ERRORS_LOG.");
    }
}
