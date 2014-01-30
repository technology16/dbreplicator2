/**
 * 
 */
package ru.taximaxim.dbreplicator2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
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
    public void testForeignKey() throws SQLException, ClassNotFoundException, IOException, InterruptedException {
        //Проверка внешних ключей
        LOG.info("Проверка внешних ключей");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        Helper.executeSqlFromFile(conn, "sql_foreign_key.sql");
        
        worker.run();
        Thread.sleep(REPLICATION_DELAY*10);
        
        errorsReplicationTimeWatchgdog.run();
        errorsCountWatchdogWorker.run();

        worker.run();
        Thread.sleep(REPLICATION_DELAY*10);
        errorsReplicationTimeWatchgdog.run();
        
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table2");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table2");
        if(listSource.size() == listDest.size()) {
            for (int i = 0; i < listSource.size(); i++) {
                long delta = listDest.get(i)._time.getTime() - listSource.get(i)._time.getTime();
                LOG.info("Rasfar: "+delta);
            }
        }
        Helper.AssertEquals(listSource, listDest);

        listSource = Helper.InfoTest(conn, "t_table3");
        listDest   = Helper.InfoTest(connDest, "t_table3");
        Helper.AssertEquals(listSource, listDest);
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
