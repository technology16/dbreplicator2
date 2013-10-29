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
        Helper.executeSqlFromFile(connDest, "importDest.sql");
        
        //int count = Helper.InfoCount(conn, "rep2_superlog");
        
        RunnerService runnerService = new RunnerService(sessionFactory);
        
        Runnable worker = new WorkerThread(runnerService.getRunner(1));
        
        worker.run();
        
        int count_rep2_superlog = Helper.InfoCount(conn, "rep2_superlog");
        if(count_rep2_superlog!=0) {
            LOG.error("Таблица rep2_superlog должна быть пустой: count = " + count_rep2_superlog);
        }
        Assert.assertEquals(count_rep2_superlog, 0);

        int count_rep2_workpool_data = Helper.InfoCount(conn, "rep2_workpool_data");
        if(count_rep2_workpool_data!=0) {
            LOG.error("Таблица rep2_workpool_data должна быть пустой: count = " + count_rep2_workpool_data);
        }
        Assert.assertEquals(count_rep2_workpool_data, 0);
        
//        if(count_rep2_workpool_data!=count) {
//            LOG.error(String.format("кол-во записи в таблице rep2_workpool_data ",
//                    "не равны rep2_superlog до удаления: [ %s == %s ]" , count_rep2_workpool_data , count));
//        }
//        Assert.assertEquals(count_rep2_workpool_data, count);
        
        List<MyTablesType> listSource = Helper.InfoTest(conn, "t_table");
        List<MyTablesType> listDest   = Helper.InfoTest(connDest, "t_table");

//        LOG.info("<======Inception======>");
//        Helper.InfoList(listSource);
//        LOG.info("=======Inception=======");
//        Helper.InfoList(listDest);
//        LOG.info(">======Inception======<");
        
        if(listSource.size() != listDest.size()) {
            LOG.error(String.format("Количество записей не равны [%s == %s]", listSource.size(), listDest.size()));
        }
        Assert.assertEquals(listSource.size(), listDest.size());
        
        if(!listSource.equals(listDest)) {
            LOG.info("====================================================================");
            for (int i = 0; i < listSource.size(); i++) {
                
                if(listSource.get(i)._int != listDest.get(i)._int) {
                    LOG.error(String.format("_int [%s == %s]", listSource.get(i)._int, listDest.get(i)._int));
                }
                Assert.assertEquals(listSource.get(i)._int, listDest.get(i)._int);
                
                if(listSource.get(i)._boolean != listDest.get(i)._boolean) {
                    LOG.error(String.format("_boolean [%s == %s]", listSource.get(i)._boolean, listDest.get(i)._boolean));
                }
                Assert.assertEquals(listSource.get(i)._boolean, listDest.get(i)._boolean);
                
                if(!listSource.get(i)._long.equals(listDest.get(i)._long)){
                    LOG.error(String.format("_long [%s == %s]", listSource.get(i)._long, listDest.get(i)._long));
                }
                Assert.assertEquals(listSource.get(i)._long, listDest.get(i)._long);
                
                if(listSource.get(i)._decimal != listDest.get(i)._decimal) {
                    LOG.error(String.format("_decimal [%s == %s]", listSource.get(i)._decimal, listDest.get(i)._decimal));
                }
                Assert.assertEquals(listSource.get(i)._decimal, listDest.get(i)._decimal);
                
                if(listSource.get(i)._string != listDest.get(i)._string){
                    LOG.error(String.format("_string [%s == %s]", listSource.get(i)._string, listDest.get(i)._string));
                }
                Assert.assertEquals(listSource.get(i)._string, listDest.get(i)._string);
                
                if(listSource.get(i)._byte != listDest.get(i)._byte){
                    LOG.error(String.format("_byte [%s == %s]", listSource.get(i)._byte, listDest.get(i)._byte));
                }
                Assert.assertEquals(listSource.get(i)._byte, listDest.get(i)._byte);
                
                if(!listSource.get(i)._date.equals(listDest.get(i)._date)) {
                    LOG.error(String.format("_date [%s == %s]", listSource.get(i)._date, listDest.get(i)._date));
                }
                Assert.assertEquals(listSource.get(i)._date, listDest.get(i)._date);
                
                if(!listSource.get(i)._time.equals(listDest.get(i)._time)){
                    LOG.error(String.format("_time [%s == %s]", listSource.get(i)._time, listDest.get(i)._time));
                }
                Assert.assertEquals(listSource.get(i)._time, listDest.get(i)._time);
                
                if(!listSource.get(i)._timestamp.equals(listDest.get(i)._timestamp)){
                    LOG.error(String.format("_timestamp [%s == %s]", listSource.get(i)._timestamp, listDest.get(i)._timestamp));
                }
                Assert.assertEquals(listSource.get(i)._timestamp, listDest.get(i)._timestamp);
                
                if(listSource.get(i)._double != listDest.get(i)._double) {
                    LOG.error(String.format("_double [%s == %s]", listSource.get(i)._double, listDest.get(i)._double));
                    Assert.assertEquals(1, 0);
                }
               
                if(listSource.get(i)._float != listDest.get(i)._float) {
                    LOG.error(String.format("_float [%s == %s]", listSource.get(i)._float, listDest.get(i)._float));
                    Assert.assertEquals(1, 0);
                }
            }
            LOG.info("====================================================================");
        }
        
        conn.close();
        connDest.close();
    }
    
    /**
     * Создание триггера
     */
    public void createTrigger(Connection conn)
            throws SQLException, ClassNotFoundException {
        Helper.createTrigger(conn, "t_table");
    }
}
