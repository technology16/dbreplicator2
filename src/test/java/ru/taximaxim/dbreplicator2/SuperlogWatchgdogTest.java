/**
 * 
 */
package ru.taximaxim.dbreplicator2;

import static org.junit.Assert.assertTrue;

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
 * @author mardanov_rm
 *
 */
public class SuperlogWatchgdogTest extends AbstractReplicationTest {

protected static final Logger LOG = Logger.getLogger(SuperlogWatchgdogTest.class);
    
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 1500;

    protected static Runnable errorsSuperlogWatchgdog = null;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("src/test/resources/hibernateIntegrityReplicatedData.cfg.xml", null, "importRep2.sql", "importSource.sql", "importDest.sql");
        initRunners();
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        close();
    }
    
    /**
     * Проверка вставки 
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException 
     */
    @Test
    public void testInsert() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
      //Проверка вставки
        Helper.executeSqlFromFile(conn, "sql_insert_error_tab.sql");   
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        errorsSuperlogWatchgdog.run();

        Helper.InfoSelect(conn,  "rep2_superlog");
        
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");
        Helper.AssertEquals(listSource, listDest);
        
        listSource = Helper.InfoTest(conn, "t_table1");
        listDest   = Helper.InfoTest(connDest, "t_table1");
        Helper.AssertEquals(listSource, listDest);
        
        int count = Helper.InfoCount(conn,  "rep2_superlog");
        assertTrue(String.format("Количество записей не должно быть пустым [%s != 0]", count), 0 != count);
    }
    
    /**
     * Инициализация раннеров
     */
    public static void initRunners() {
        RunnerService runnerService = new RunnerService(sessionFactory);

        worker = new WorkerThread(runnerService.getRunner(1));
        errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(7));
        errorsSuperlogWatchgdog = new WorkerThread(runnerService.getRunner(15));
    }
}
