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
public class ReplicationTimeWatchgdogTest extends AbstractReplicationTest {

protected static final Logger LOG = Logger.getLogger(ReplicationTimeWatchgdogTest.class);
    
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 100;

    protected static Runnable errorsReplicationTimeWatchgdog = null;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("importRepTimeWatch.sql", "init_db/importRep2.sql", "init_db/importSource.sql", "init_db/importDest.sql");
        initRunners();
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        close();
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
     * 
     * 
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException 
     */
    
    @Test
    public void testErrorsReplicationTimeWatchgdog() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        //Проверка внешних ключей
        LOG.info("Проверка внешних ключей");
        Helper.executeSqlFromFile(conn, "sql_query/sql_insert.sql");
        Helper.executeSqlFromFile(conn, "sql_query/sql_update.sql");
        
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        
        errorsReplicationTimeWatchgdog.run();
        errorsCountWatchdogWorker.run();
    }
    
    /**
     * Инициализация раннеров
     */
    public static void initRunners() {
        RunnerService runnerService = new RunnerService(sessionFactory);

        worker = new WorkerThread(runnerService.getRunner(1));
        errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(7));
        errorsReplicationTimeWatchgdog = new WorkerThread(runnerService.getRunner(10));
    }
}
