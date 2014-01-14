/**
 * 
 */
package ru.taximaxim.dbreplicator2;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.stats.StatsService;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * @author mardanov_rm
 *
 */
public class StatsServiceTest {
    
    protected static final Logger LOG = Logger.getLogger(StatsServiceTest.class);
    protected static SessionFactory sessionFactory;
    protected static Session session;
    protected static ConnectionFactory connectionFactory;
    protected static StatsService statsService = null;
    protected static Timestamp dateStart = null;
    protected static Timestamp dateEnd = null;
    protected static Timestamp dateMidde = null;
    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        sessionFactory = Core.getSessionFactory();
        session = sessionFactory.openSession();
        connectionFactory = Core.getConnectionFactory();
        statsService = Core.getStatsService();
        initialization();
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        if(session!=null) {
            session.close();
        }
        Core.connectionFactoryClose();
        Core.sessionFactoryClose();
        Core.statsServiceClose();
    }

    //c_date, c_type, id_strategy, id_table, c_count
    
    @Test
    public void testGetStatByTypeTablePeriod() throws ClassNotFoundException, SQLException {
        //int type, String tableName,  Timestamp dateStart, Timestamp dateEnd
        try (
                ResultSet result1 = statsService.getStat(0, "T_PLACE", dateStart, dateEnd);
                ResultSet result2 = statsService.getStat(0, "T_BASES", dateStart, dateEnd);
                ResultSet result3 = statsService.getStat(0, "T_HOUSE", dateStart, dateEnd);
                
                ResultSet result4 = statsService.getStat(1, "T_PLACE", dateStart, dateEnd);
                ResultSet result5 = statsService.getStat(1, "T_BASES", dateStart, dateEnd);
                ResultSet result6 = statsService.getStat(1, "T_HOUSE", dateStart, dateEnd);
            ) {
            
            assertTrueStatsRow1(result4, false);            
            assertTrueStatsRow2(result2, false);
            assertTrueStatsRow3(result6, false);
            assertTrueStatsRow4(result1, false);
            assertTrueStatsRow5(result6, false);
            assertTrueStatsRow6(result1, false);
            assertTrueStatsRow7(result5, false);
            assertTrueStatsRow8(result3, false);
        } 
    }
    
    @Test
    public void testgetStatByType() throws ClassNotFoundException, SQLException {
        //int type
        try (
                ResultSet result1 = statsService.getStat(0);
                ResultSet result2 = statsService.getStat(1);
            ) {
            assertTrueStatsRow1(result2, false);            
            assertTrueStatsRow2(result1, false);
            assertTrueStatsRow3(result2, false);
            assertTrueStatsRow4(result1, false);
            assertTrueStatsRow5(result2, false);
            assertTrueStatsRow6(result1, false);
            assertTrueStatsRow7(result2, false);
            assertTrueStatsRow8(result1, false);
        }
    }
    
    @Test
    public void testgetStatByTypeTable() throws ClassNotFoundException, SQLException {
        //int type, String tableName
        try (
                ResultSet result1 = statsService.getStat(0, "T_PLACE");
                ResultSet result2 = statsService.getStat(0, "T_BASES");
                ResultSet result3 = statsService.getStat(0, "T_HOUSE");
                
                ResultSet result4 = statsService.getStat(1, "T_PLACE");
                ResultSet result5 = statsService.getStat(1, "T_BASES");
                ResultSet result6 = statsService.getStat(1, "T_HOUSE");
            ) {
            
            assertTrueStatsRow1(result4, false);            
            assertTrueStatsRow2(result2, false);
            assertTrueStatsRow3(result6, false);
            assertTrueStatsRow4(result1, false);
            assertTrueStatsRow5(result6, false);
            assertTrueStatsRow6(result1, false);
            assertTrueStatsRow7(result5, false);
            assertTrueStatsRow8(result3, false);
        } 
    }
    
    @Test
    public void testgetStatByTypePeriod() throws ClassNotFoundException, SQLException {
        //int type, Timestamp dateStart, Timestamp dateEnd
        try (
                ResultSet result1 = statsService.getStat(0, dateStart, dateEnd);
                ResultSet result2 = statsService.getStat(1, dateStart, dateEnd);
            ) {
            
            assertTrueStatsRow1(result2, false);            
            assertTrueStatsRow2(result1, false);
            assertTrueStatsRow3(result2, false);
            assertTrueStatsRow4(result1, false);
            assertTrueStatsRow5(result2, false);
            assertTrueStatsRow6(result1, false);
            assertTrueStatsRow7(result2, false);
            assertTrueStatsRow8(result1, false);
        } 
    }

    @Test
    public void testgetStatByTypeTablePeriodStartMidde() throws ClassNotFoundException, SQLException {
        //int type, String tableName,  Timestamp dateStart, Timestamp dateEnd
        try (
                ResultSet result1 = statsService.getStat(0, "T_PLACE", dateStart, dateMidde);
                ResultSet result2 = statsService.getStat(0, "T_BASES", dateStart, dateMidde);
                ResultSet result3 = statsService.getStat(0, "T_HOUSE", dateStart, dateMidde);
                
                ResultSet result4 = statsService.getStat(1, "T_PLACE", dateStart, dateMidde);
                ResultSet result5 = statsService.getStat(1, "T_BASES", dateStart, dateMidde);
                ResultSet result6 = statsService.getStat(1, "T_HOUSE", dateStart, dateMidde);
            ) {
            
            assertTrueStatsRow1(result4, false);            
            assertTrueStatsRow2(result2, false);
            assertTrueStatsRow3(result6, false);
            assertTrueStatsRow4(result1, false);
            assertTrueStatsRow5(result6, true);
            assertTrueStatsRow6(result1, true);
            assertTrueStatsRow7(result5, true);
            assertTrueStatsRow8(result3, true);
        } 
    }
    
    @Test
    public void testgetStatByTypeTablePeriodMiddeEnd() throws ClassNotFoundException, SQLException {
        //int type, String tableName,  Timestamp dateStart, Timestamp dateEnd
        try (
                ResultSet result1 = statsService.getStat(0, "T_PLACE", dateMidde, dateEnd);
                ResultSet result2 = statsService.getStat(0, "T_BASES", dateMidde, dateEnd);
                ResultSet result3 = statsService.getStat(0, "T_HOUSE", dateMidde, dateEnd);
                
                ResultSet result4 = statsService.getStat(1, "T_PLACE", dateMidde, dateEnd);
                ResultSet result5 = statsService.getStat(1, "T_BASES", dateMidde, dateEnd);
                ResultSet result6 = statsService.getStat(1, "T_HOUSE", dateMidde, dateEnd);
            ) {
            
            assertTrueStatsRow1(result4, true);            
            assertTrueStatsRow2(result2, true);
            assertTrueStatsRow5(result6, false);
            assertTrueStatsRow6(result1, false);
            assertTrueStatsRow5(result6, true);
            assertTrueStatsRow6(result1, true);
            assertTrueStatsRow7(result5, false);
            assertTrueStatsRow8(result3, false);
        } 
    }
    
    /**
     * Инициализация
     * @throws InterruptedException 
     */
    public static void initialization() throws ClassNotFoundException, SQLException, IOException, InterruptedException{
        LOG.info("initialization");
        String source = "source";
        Connection conn = connectionFactory.getConnection(source);
        Helper.executeSqlFromFile(conn, "importRep2.sql");
        
        dateStart = new Timestamp(new Date().getTime());
        Thread.sleep(500);
        statsService.writeStat(new Timestamp(new Date().getTime()), 1, 2, "T_PLACE", 10);
        Thread.sleep(500);
        statsService.writeStat(new Timestamp(new Date().getTime()), 0, 3, "T_BASES", 20);
        Thread.sleep(500);
        statsService.writeStat(new Timestamp(new Date().getTime()), 1, 4, "T_HOUSE", 30);
        Thread.sleep(500);
        statsService.writeStat(new Timestamp(new Date().getTime()), 0, 5, "T_PLACE", 40);
        Thread.sleep(500);
        dateMidde = new Timestamp(new Date().getTime());
        Thread.sleep(500);
        statsService.writeStat(new Timestamp(new Date().getTime()), 1, 6, "T_HOUSE", 50);
        Thread.sleep(500);
        statsService.writeStat(new Timestamp(new Date().getTime()), 0, 7, "T_PLACE", 60);
        Thread.sleep(500);
        statsService.writeStat(new Timestamp(new Date().getTime()), 1, 8, "T_BASES", 70);
        Thread.sleep(500);
        statsService.writeStat(new Timestamp(new Date().getTime()), 0, 9, "T_HOUSE", 80);
        Thread.sleep(500);
        dateEnd = new Timestamp(new Date().getTime());
    }
    
    public void assertTrueStatsRow1(ResultSet result, boolean row) throws SQLException {
        if(result.next()) {
            assertTrue("Вставленные данные не совпадают", result.getInt("c_type") == 1);
            assertTrue("Вставленные данные не совпадают", result.getInt("id_strategy") == 2);
            assertTrue("Вставленные данные не совпадают", result.getString("id_table").equals("T_PLACE"));
            assertTrue("Вставленные данные не совпадают", result.getInt("c_count") == 10);
        } else {
            assertTrue("Отсутствует запись rep2_statistics id = 1", row);
        }
    }
    
    public void assertTrueStatsRow2(ResultSet result, boolean row) throws SQLException {
        if(result.next()) {
            assertTrue("Вставленные данные не совпадают", result.getInt("c_type") == 0);
            assertTrue("Вставленные данные не совпадают", result.getInt("id_strategy") == 3);
            assertTrue("Вставленные данные не совпадают", result.getString("id_table").equals("T_BASES"));
            assertTrue("Вставленные данные не совпадают", result.getInt("c_count") == 20);
        } else {
            assertTrue("Отсутствует запись rep2_statistics id = 2", row);
        }
    }
    
    public void assertTrueStatsRow3(ResultSet result, boolean row) throws SQLException {
        if(result.next()) {
            assertTrue("Вставленные данные не совпадают", result.getInt("c_type") == 1);
            assertTrue("Вставленные данные не совпадают", result.getInt("id_strategy") == 4);
            assertTrue("Вставленные данные не совпадают", result.getString("id_table").equals("T_HOUSE"));
            assertTrue("Вставленные данные не совпадают", result.getInt("c_count") == 30);
        } else {
            assertTrue("Отсутствует запись rep2_statistics id = 3", row);
        }
    }
    
    public void assertTrueStatsRow4(ResultSet result, boolean row) throws SQLException {
        if(result.next()) {
            assertTrue("Вставленные данные не совпадают", result.getInt("c_type") == 0);
            assertTrue("Вставленные данные не совпадают", result.getInt("id_strategy") == 5);
            assertTrue("Вставленные данные не совпадают", result.getString("id_table").equals("T_PLACE"));
            assertTrue("Вставленные данные не совпадают", result.getInt("c_count") == 40);
        } else {
            assertTrue("Отсутствует запись rep2_statistics id = 4", row);
        }
    }
    
    public void assertTrueStatsRow5(ResultSet result, boolean row) throws SQLException {
        if(result.next()) {
            assertTrue("Вставленные данные не совпадают", result.getInt("c_type") == 1);
            assertTrue("Вставленные данные не совпадают", result.getInt("id_strategy") == 6);
            assertTrue("Вставленные данные не совпадают", result.getString("id_table").equals("T_HOUSE"));
            assertTrue("Вставленные данные не совпадают", result.getInt("c_count") == 50);
        } else {
            assertTrue("Отсутствует запись rep2_statistics id = 5", row);
        }
    }
    
    public void assertTrueStatsRow6(ResultSet result, boolean row) throws SQLException {
        if(result.next()) {
            assertTrue("Вставленные данные не совпадают", result.getInt("c_type") == 0);
            assertTrue("Вставленные данные не совпадают", result.getInt("id_strategy") == 7);
            assertTrue("Вставленные данные не совпадают", result.getString("id_table").equals("T_PLACE"));
            assertTrue("Вставленные данные не совпадают", result.getInt("c_count") == 60);
        } else {
            assertTrue("Отсутствует запись rep2_statistics id = 6", row);
        }
    }
    
    public void assertTrueStatsRow7(ResultSet result, boolean row) throws SQLException {
        if(result.next()) {
            assertTrue("Вставленные данные не совпадают", result.getInt("c_type") == 1);
            assertTrue("Вставленные данные не совпадают", result.getInt("id_strategy") == 8);
            assertTrue("Вставленные данные не совпадают", result.getString("id_table").equals("T_BASES"));
            assertTrue("Вставленные данные не совпадают", result.getInt("c_count") == 70);
        } else {
            assertTrue("Отсутствует запись rep2_statistics id = 7", row);
        }
    }
    
    public void assertTrueStatsRow8(ResultSet result, boolean row) throws SQLException {
        if(result.next()) {
            assertTrue("Вставленные данные не совпадают", result.getInt("c_type") == 0);
            assertTrue("Вставленные данные не совпадают", result.getInt("id_strategy") == 9);
            assertTrue("Вставленные данные не совпадают", result.getString("id_table").equals("T_HOUSE"));
            assertTrue("Вставленные данные не совпадают", result.getInt("c_count") == 80);
        } else {
            assertTrue("Отсутствует запись rep2_statistics id = 8", row);
        }
    }
}
