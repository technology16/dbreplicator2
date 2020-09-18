/**
 *
 */
package ru.taximaxim.dbreplicator2;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractReplicationTest;
import ru.taximaxim.dbreplicator2.model.RunnerService;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;
/**
 * @author mardanov_rm
 *
 */
public class SuperlogWatchgdogTest extends AbstractReplicationTest {

    protected static final Logger LOG = Logger.getLogger(SuperlogWatchgdogTest.class);

    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 100;

    protected static Runnable errorsSuperlogWatchgdog = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("importIntegrityReplicatedData.sql", "init_db/importRep2.sql",
                "init_db/importSource.sql", "init_db/importDest.sql");
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
        Helper.executeSqlFromFile(source, "sql_query/sql_insert_error_tab.sql");
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        errorsSuperlogWatchgdog.run();

        Helper.InfoSelect(source, "rep2_superlog");

        verifyTable("t_table");
        verifyTable("t_table1");

        Helper.assertNotEmptyTable(source, "rep2_superlog");
    }

    /**
     * Инициализация раннеров
     */
    public static void initRunners() {
        RunnerService runnerService = new RunnerService(sessionFactory);

        worker = new WorkerThread(runnerService.getRunner(1));
        errorsSuperlogWatchgdog = new WorkerThread(runnerService.getRunner(15));
    }
}
