/**
 * 
 */
package ru.taximaxim.dbreplicator2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.model.RunnerService;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * @author mardanov_rm
 *
 */
public class ReplicationTimeWatchgdogTest {

protected static final Logger LOG = Logger.getLogger(ReplicationTimeWatchgdogTest.class);
    
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 1500;
    
    protected static SessionFactory sessionFactory;
    protected static Session session;
    protected static ConnectionFactory connectionFactory;
    protected static Connection conn = null;
    protected static Connection connDest = null;
    protected static Runnable worker = null;
    protected static Runnable errorsCountWatchdogWorker = null;
    protected static Runnable errorsReplicationTimeWatchgdog = null;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Core.getConfiguration("src/test/resources/hibernateRepTimeWatch.cfg.xml");
        sessionFactory = Core.getSessionFactory();
        session = sessionFactory.openSession();
        connectionFactory = Core.getConnectionFactory();
        initialization();
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        if(conn!=null)
            conn.close();
        if(connDest!=null)
            connDest.close();
        if(session!=null)
            session.close();
        Core.connectionFactoryClose();
        Core.sessionFactoryClose();
        Core.threadPoolClose();
        Core.statsServiceClose();
        Core.tasksPoolClose();
        Core.taskSettingsServiceClose(); 
        Core.configurationClose();
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
        Helper.executeSqlFromFile(conn, "sql_insert.sql");
        Helper.executeSqlFromFile(conn, "sql_update.sql");
        
        worker.run();
        Thread.sleep(REPLICATION_DELAY);
        
        errorsReplicationTimeWatchgdog.run();
        errorsCountWatchdogWorker.run();
    }
    
    /**
     * Инициализация
     */
    public static void initialization() throws ClassNotFoundException, SQLException, IOException{
        LOG.info("initialization");
        String source = "source";
        conn = connectionFactory.getConnection(source);
        
        Helper.executeSqlFromFile(conn, "importRep2.sql");
        Helper.executeSqlFromFile(conn, "importSource.sql");
        
        String dest = "dest";
        connDest = connectionFactory.getConnection(dest);
        Helper.executeSqlFromFile(connDest, "importRep2.sql");
        Helper.executeSqlFromFile(connDest, "importDest.sql");
        
        RunnerService runnerService = new RunnerService(sessionFactory);

        worker = new WorkerThread(runnerService.getRunner(1));
        errorsCountWatchdogWorker = new WorkerThread(runnerService.getRunner(7));
        errorsReplicationTimeWatchgdog = new WorkerThread(runnerService.getRunner(10));
    }
}
