package ru.taximaxim.dbreplicator2.strategies.errors;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import ru.taximaxim.dbreplicator2.abstracts.AbstractReplicationTest;
import ru.taximaxim.dbreplicator2.model.RunnerService;
import ru.taximaxim.dbreplicator2.replica.strategies.errors.CountWatchgdog;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ Logger.class })
@PowerMockIgnore("javax.management.*")
public class CountWatchgdogTest extends AbstractReplicationTest {
    protected static final Logger LOG = Logger.getLogger(CountWatchgdogTest.class);

    private static final Logger spyLogger = spy(Logger.getLogger(CountWatchgdog.class));;
    private static RunnerService runnerService;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("errors/CountWatchgdogTest.sql", "init_db/importRep2.sql",
                "init_db/importSource.sql", "init_db/importDest.sql");

        runnerService = new RunnerService(sessionFactory);

        // Мокируем логгер для проверки сообщений об ошибках
        PowerMockito.spy(Logger.class);
        PowerMockito.doReturn(spyLogger).when(Logger.class, "getLogger",
                CountWatchgdog.class);

    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        close();
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
     * Вставлем ошибки таблицы
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
     * Вставлем ошибки таблицы
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
     * Вставлем ошибки
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
     * Вставлем ошибки
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
     * Вставлем отработанные ошибки
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
     * Вставлем отработанные ошибки таблицы
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
     * Вставлем отработанные ошибки раннера
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

        // Задаем в явном виде текущее время. Используем .0 что бы избежать
        // проблем с округлением
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0");
        String now = formatter.format(new Timestamp(new Date().getTime()));

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

        InOrder inOrder = Mockito.inOrder(spyLogger);
        inOrder.verify(spyLogger).error("\n" + "\n"
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
        // Задаем в явном виде текущее время. Используем .0 что бы избежать
        // проблем с округлением
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0");
        String now = formatter.format(new Timestamp(new Date().getTime()));

        // Добавляем исправленные ошибки
        clearErrorsLog();
        insertFixedError(1, "table", now, 1L);
        insertFixedTableError(2, "table", now, 2L);
        insertFixedRunnerError(3, now, 3L);

        Mockito.reset(spyLogger);

        // Запускаем CountWatchgdog
        WorkerThread errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(1));
        errorsCountWatchdogWorker.run();

        // проверяем что не было сообщений об ошибках
        InOrder inOrder = Mockito.inOrder(spyLogger);
        inOrder.verifyNoMoreInteractions();
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

        // Задаем в явном виде текущее время. Используем .0 что бы избежать
        // проблем с округлением
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0");
        String now = formatter.format(new Timestamp(new Date().getTime()));

        clearErrorsLog();
        insertActiveErrors(1, "table", now, 1L, 9L);
        insertActiveTableError(2, "table", now, 10L);
        insertActiveRunnerError(3, now, 11L);

        // Запускаем CountWatchgdog
        WorkerThread errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(2));
        errorsCountWatchdogWorker.run();

        InOrder inOrder = Mockito.inOrder(spyLogger);
        inOrder.verify(spyLogger).error("\n" + "\n" + "В error превышен лимит в 0 ошибок!\n"
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
        // Задаем в явном виде текущее время. Используем .0 что бы избежать
        // проблем с округлением
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.0");

        clearErrorsLog();
        
        // Добавляем ошибки старше 10 секунд
        String now10 = formatter.format(new Timestamp(System.currentTimeMillis() - 10000));
        insertActiveError(1, "table", now10, 1L);
        insertActiveTableError(2, "table", now10, 2L);
        insertActiveRunnerError(3, now10, 3L);
        
        // Добавляем ошибки старше 3 секунд
        String now3 = formatter.format(new Timestamp(System.currentTimeMillis() - 3000));
        insertActiveError(1, "table", now3, 4L);
        insertActiveTableError(2, "table", now3, 5L);
        insertActiveRunnerError(3, now3, 6L);

        // Запускаем CountWatchgdog
        WorkerThread errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(3));
        errorsCountWatchdogWorker.run();


        InOrder inOrder = Mockito.inOrder(spyLogger);
        inOrder.verify(spyLogger).error("\n" + 
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
