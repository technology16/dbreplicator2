package ru.taximaxim.dbreplicator2;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.model.RunnerService;

public class H2CopyTableDataTest {
    protected static final Logger LOG = Logger.getLogger(H2CopyTableDataTest.class);
    protected static SessionFactory sessionFactory;
    protected static Session session;
    protected static ConnectionFactory connectionFactory;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        sessionFactory = Application.getSessionFactory();
        session = sessionFactory.openSession();
        connectionFactory = Application.getConnectionFactory();
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        connectionFactory.close();
        session.close();
        sessionFactory.close();
    }
    
    @Ignore
    @Test
    public void testTableDataTest() throws SQLException, ClassNotFoundException, IOException {

        LOG.debug("Start: ");
        String source = "source";
        Connection conn = connectionFactory.getConnection(source);
        
        Helper.executeSqlFromFile(conn, "importRep2.sql");
        Helper.executeSqlFromFile(conn, "importSource.sql");
        createTrigger(conn);
        Helper.executeSqlFromFile(conn, "importSourceData.sql");
        

        String dest = "dest";
        Connection connDest = connectionFactory.getConnection(dest);
        Helper.executeSqlFromFile(connDest, "importSource.sql");
        
        int count = Helper.InfoCount(conn, "rep2_superlog");
        
        RunnerService runnerService = new RunnerService(sessionFactory);
        
        Runnable worker = new WorkerThread(runnerService.getRunner(1));
        
        worker.run();
        
        LOG.info("Таблица rep2_superlog должна быть пустой");
        int count_rep2_superlog = Helper.InfoCount(conn, "rep2_superlog");

        Assert.assertEquals(count_rep2_superlog, 0);
        LOG.info("Таблица rep2_workpool_data");
        int count_rep2_workpool_data = Helper.InfoCount(conn, "rep2_workpool_data");
        Assert.assertNotEquals(count_rep2_workpool_data, 0);
        
        Assert.assertEquals(count_rep2_workpool_data, count);
        
        List<MyTablesType> listSource = Helper.InfoTest(conn, "T_Source");
        List<MyTablesType> listDest   = Helper.InfoTest(conn, "T_Dest");
        
        LOG.debug(String.format("size [%s == %s]", listSource.size(), listDest.size()));
        Assert.assertEquals(listSource.size(), listDest.size());
        
        if(!listSource.equals(listDest)) {
            for (int i = 0; i < listSource.size(); i++) {
                Assert.assertEquals(listSource.get(i)._int, listDest.get(i)._int);
                Assert.assertEquals(listSource.get(i)._boolean, listDest.get(i)._boolean);
                Assert.assertEquals(listSource.get(i)._long, listDest.get(i)._long);
                Assert.assertEquals(listSource.get(i)._decimal, listDest.get(i)._decimal);
                Assert.assertEquals(listSource.get(i)._string, listDest.get(i)._string);
                Assert.assertEquals(listSource.get(i)._byte, listDest.get(i)._byte);
                Assert.assertEquals(listSource.get(i)._date, listDest.get(i)._date);
                if(listSource.get(i)._double != listDest.get(i)._double) {
                    Assert.assertEquals(1, 0);
                }
                if(listSource.get(i)._float != listDest.get(i)._float) {
                    Assert.assertEquals(1, 0);
                }
            }
        }
        
        Assert.assertEquals(listSource, listDest);
        
        conn.close();
        connDest.close();
    }
    
    /**
     * Создание триггера
     */
    public void createTrigger(Connection conn)
            throws SQLException, ClassNotFoundException {
        Helper.createTrigger(conn, "T_TABLE1");
        Helper.createTrigger(conn, "T_TABLE2");
        Helper.createTrigger(conn, "T_TABLE3");
        Helper.createTrigger(conn, "T_TABLE4");
        Helper.createTrigger(conn, "T_TABLE5");
        Helper.createTrigger(conn, "T_TABLE6");
        Helper.createTrigger(conn, "T_TABLE7");
        Helper.createTrigger(conn, "T_TABLE8");
        Helper.createTrigger(conn, "T_TABLE9");
        Helper.createTrigger(conn, "T_TABLE0");
    }
}
