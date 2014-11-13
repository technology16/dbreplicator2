/**
 * 
 */
package ru.taximaxim.dbreplicator2;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractSecondTest;
import ru.taximaxim.dbreplicator2.stats.StatsService;

/**
 * @author mardanov_rm
 * 
 */
public class StatsServiceTest extends AbstractSecondTest {
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 500;
    protected static final Logger LOG = Logger.getLogger(StatsServiceTest.class);

    protected static Timestamp dateStart = null;
    protected static Timestamp dateEnd = null;
    protected static Timestamp dateMidde = null;

    private static final String T_PLACE = "T_PLACE";
    private static final String T_BASES = "T_BASES";
    private static final String T_HOUSE = "T_HOUSE";
    private static final String C_TYPE = "c_type";
    private static final String ID_STRATEGY = "id_strategy";
    private static final String ID_TABLE = "id_table";
    private static final String C_COUNT = "c_count";
    
    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("importRep2.sql", null, null);     
        initialization();
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        close();
    }

    /**
     * Инициализация
     * 
     * @throws InterruptedException
     */
    public static void initialization() throws ClassNotFoundException, SQLException, IOException, InterruptedException {
        dateStart = new Timestamp(new Date().getTime());
        Thread.sleep(REPLICATION_DELAY);
        statsService.writeStat(new Timestamp(new Date().getTime()), StatsService.TYPE_SUCCESS, 2, T_PLACE, 10);
        Thread.sleep(REPLICATION_DELAY);
        statsService.writeStat(new Timestamp(new Date().getTime()), StatsService.TYPE_ERROR, 3, T_BASES, 20);
        Thread.sleep(REPLICATION_DELAY);
        statsService.writeStat(new Timestamp(new Date().getTime()), StatsService.TYPE_SUCCESS, 4, T_HOUSE, 30);
        Thread.sleep(REPLICATION_DELAY);
        statsService.writeStat(new Timestamp(new Date().getTime()), StatsService.TYPE_ERROR, 5, T_PLACE, 40);
        Thread.sleep(REPLICATION_DELAY);
        dateMidde = new Timestamp(new Date().getTime());
        Thread.sleep(REPLICATION_DELAY);
        statsService.writeStat(new Timestamp(new Date().getTime()), StatsService.TYPE_SUCCESS, 6, T_HOUSE, 50);
        Thread.sleep(REPLICATION_DELAY);
        statsService.writeStat(new Timestamp(new Date().getTime()), StatsService.TYPE_ERROR, 7, T_PLACE, 60);
        Thread.sleep(REPLICATION_DELAY);
        statsService.writeStat(new Timestamp(new Date().getTime()), StatsService.TYPE_SUCCESS, 8, T_BASES, 70);
        Thread.sleep(REPLICATION_DELAY);
        statsService.writeStat(new Timestamp(new Date().getTime()), StatsService.TYPE_ERROR, 9, T_HOUSE, 80);
        Thread.sleep(REPLICATION_DELAY);
        dateEnd = new Timestamp(new Date().getTime());
    }
    
    @Test
    public void testGetStatByTypeTablePeriod() throws ClassNotFoundException,
            SQLException {
        // int type, String tableName, Timestamp dateStart, Timestamp dateEnd
        List<Map<String, Object>> result1 = statsService.getStat(StatsService.TYPE_ERROR, T_PLACE, dateStart, dateEnd);
        List<Map<String, Object>> result2 = statsService.getStat(StatsService.TYPE_ERROR, T_BASES, dateStart, dateEnd);
        List<Map<String, Object>> result3 = statsService.getStat(StatsService.TYPE_ERROR, T_HOUSE, dateStart, dateEnd);
        List<Map<String, Object>> result4 = statsService.getStat(StatsService.TYPE_SUCCESS, T_PLACE, dateStart, dateEnd);
        List<Map<String, Object>> result5 = statsService.getStat(StatsService.TYPE_SUCCESS, T_BASES, dateStart, dateEnd);
        List<Map<String, Object>> result6 = statsService.getStat(StatsService.TYPE_SUCCESS, T_HOUSE, dateStart, dateEnd);

        assertTrueStatsRow1(result4, false, 0);
        assertTrueStatsRow2(result2, false, 0);
        assertTrueStatsRow3(result6, false, 0);
        assertTrueStatsRow4(result1, false, 0);
        assertTrueStatsRow5(result6, false, 1);
        assertTrueStatsRow6(result1, false, 1);
        assertTrueStatsRow7(result5, false, 0);
        assertTrueStatsRow8(result3, false, 0);
    }

    @Test
    public void testgetStatByType() throws ClassNotFoundException, SQLException {
        // int type
        List<Map<String, Object>> result1 = statsService.getStat(StatsService.TYPE_ERROR);
        List<Map<String, Object>> result2 = statsService.getStat(StatsService.TYPE_SUCCESS);
        
        assertTrueStatsRow1(result2, false, 0);
        assertTrueStatsRow2(result1, false, 0);
        assertTrueStatsRow3(result2, false, 1);
        assertTrueStatsRow4(result1, false, 1);
        assertTrueStatsRow5(result2, false, 2);
        assertTrueStatsRow6(result1, false, 2);
        assertTrueStatsRow7(result2, false, 3);
        assertTrueStatsRow8(result1, false, 3);
    }

    @Test
    public void testgetStatByTypeTable() throws ClassNotFoundException, SQLException {
        // int type, String tableName
        List<Map<String, Object>> result1 = statsService.getStat(StatsService.TYPE_ERROR, T_PLACE);
        List<Map<String, Object>> result2 = statsService.getStat(StatsService.TYPE_ERROR, T_BASES);
        List<Map<String, Object>> result3 = statsService.getStat(StatsService.TYPE_ERROR, T_HOUSE);
        List<Map<String, Object>> result4 = statsService.getStat(StatsService.TYPE_SUCCESS, T_PLACE);
        List<Map<String, Object>> result5 = statsService.getStat(StatsService.TYPE_SUCCESS, T_BASES);
        List<Map<String, Object>> result6 = statsService.getStat(StatsService.TYPE_SUCCESS, T_HOUSE);

        assertTrueStatsRow1(result4, false, 0);
        assertTrueStatsRow2(result2, false, 0);
        assertTrueStatsRow3(result6, false, 0);
        assertTrueStatsRow4(result1, false, 0);
        assertTrueStatsRow5(result6, false, 1);
        assertTrueStatsRow6(result1, false, 1);
        assertTrueStatsRow7(result5, false, 0);
        assertTrueStatsRow8(result3, false, 0);
    }

    @Test
    public void testgetStatByTypePeriod() throws ClassNotFoundException, SQLException {
        // int type, Timestamp dateStart, Timestamp dateEnd
        List<Map<String, Object>> result1 = statsService.getStat(StatsService.TYPE_ERROR, dateStart, dateEnd);
        List<Map<String, Object>> result2 = statsService.getStat(StatsService.TYPE_SUCCESS, dateStart, dateEnd);

        assertTrueStatsRow1(result2, false, 0);
        assertTrueStatsRow2(result1, false, 0);
        assertTrueStatsRow3(result2, false, 1);
        assertTrueStatsRow4(result1, false, 1);
        assertTrueStatsRow5(result2, false, 2);
        assertTrueStatsRow6(result1, false, 2);
        assertTrueStatsRow7(result2, false, 3);
        assertTrueStatsRow8(result1, false, 3);
    }

    @Test
    public void testgetStatByTypeTablePeriodStartMidde() throws ClassNotFoundException, SQLException {
        // int type, String tableName, Timestamp dateStart, Timestamp dateEnd
        List<Map<String, Object>> result1 = statsService.getStat(StatsService.TYPE_ERROR, T_PLACE, dateStart, dateMidde);
        List<Map<String, Object>> result2 = statsService.getStat(StatsService.TYPE_ERROR, T_BASES, dateStart, dateMidde);
        List<Map<String, Object>> result3 = statsService.getStat(StatsService.TYPE_ERROR, T_HOUSE, dateStart, dateMidde);
        List<Map<String, Object>> result4 = statsService.getStat(StatsService.TYPE_SUCCESS, T_PLACE, dateStart, dateMidde);
        List<Map<String, Object>> result5 = statsService.getStat(StatsService.TYPE_SUCCESS, T_BASES, dateStart, dateMidde);
        List<Map<String, Object>> result6 = statsService.getStat(StatsService.TYPE_SUCCESS, T_HOUSE, dateStart, dateMidde);
        
        assertTrueStatsRow1(result4, false, 0);
        assertTrueStatsRow2(result2, false, 0);
        assertTrueStatsRow3(result6, false, 0);
        assertTrueStatsRow4(result1, false, 0);
        assertTrueStatsRow5(result6, true, 1);
        assertTrueStatsRow6(result1, true, 1);
        assertTrueStatsRow7(result5, true, 0);
        assertTrueStatsRow8(result3, true, 0);
    }

    @Test
    public void testgetStatByTypeTablePeriodMiddeEnd() throws ClassNotFoundException, SQLException {
        // int type, String tableName, Timestamp dateStart, Timestamp dateEnd
        List<Map<String, Object>> result1 = statsService.getStat(StatsService.TYPE_ERROR, T_PLACE, dateMidde, dateEnd);
        List<Map<String, Object>> result2 = statsService.getStat(StatsService.TYPE_ERROR, T_BASES, dateMidde, dateEnd);
        List<Map<String, Object>> result3 = statsService.getStat(StatsService.TYPE_ERROR, T_HOUSE, dateMidde, dateEnd);
        List<Map<String, Object>> result4 = statsService.getStat(StatsService.TYPE_SUCCESS, T_PLACE, dateMidde, dateEnd);
        List<Map<String, Object>> result5 = statsService.getStat(StatsService.TYPE_SUCCESS, T_BASES, dateMidde, dateEnd);
        List<Map<String, Object>> result6 = statsService.getStat(StatsService.TYPE_SUCCESS, T_HOUSE, dateMidde, dateEnd);

        assertTrueStatsRow1(result4, true, 0);
        assertTrueStatsRow2(result2, true, 0);
        assertTrueStatsRow5(result6, false, 0);
        assertTrueStatsRow6(result1, false, 0);
        assertTrueStatsRow5(result6, true, 1);
        assertTrueStatsRow6(result1, true, 1);
        assertTrueStatsRow7(result5, false, 0);
        assertTrueStatsRow8(result3, false, 0);
    }

    public void assertTrueStatsRow1(List<Map<String, Object>> resultList, boolean row,
            int index) throws SQLException {
        try {
            Map<String, Object> result = resultList.get(index);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt((result.get(C_TYPE).toString())) == 1);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(ID_STRATEGY).toString()) == 2);
            assertTrue("Вставленные данные не совпадают", result.get(ID_TABLE).toString().equals(T_PLACE));
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(C_COUNT).toString()) == 10);
        } catch (IndexOutOfBoundsException e) {
            assertTrue("Отсутствует запись rep2_statistics id = 1", row);
        }
    }

    public void assertTrueStatsRow2(List<Map<String, Object>> resultList, boolean row,
            int index) throws SQLException {
        try {
            Map<String, Object> result = resultList.get(index);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt((result.get(C_TYPE).toString())) == 0);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(ID_STRATEGY).toString()) == 3);
            assertTrue("Вставленные данные не совпадают", result.get(ID_TABLE).toString().equals(T_BASES));
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(C_COUNT).toString()) == 20);
        } catch (IndexOutOfBoundsException e) {
            assertTrue("Отсутствует запись rep2_statistics id = 2", row);
        }
    }

    public void assertTrueStatsRow3(List<Map<String, Object>> resultList, boolean row,
            int index) throws SQLException {
        try {
            Map<String, Object> result = resultList.get(index);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt((result.get(C_TYPE).toString())) == 1);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(ID_STRATEGY).toString()) == 4);
            assertTrue("Вставленные данные не совпадают", result.get(ID_TABLE).toString().equals(T_HOUSE));
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(C_COUNT).toString()) == 30);
        } catch (IndexOutOfBoundsException e) {
            assertTrue("Отсутствует запись rep2_statistics id = 3", row);
        }
    }

    public void assertTrueStatsRow4(List<Map<String, Object>> resultList, boolean row,
            int index) throws SQLException {
        try {
            Map<String, Object> result = resultList.get(index);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt((result.get(C_TYPE).toString())) == 0);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(ID_STRATEGY).toString()) == 5);
            assertTrue("Вставленные данные не совпадают", result.get(ID_TABLE).toString().equals(T_PLACE));
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(C_COUNT).toString()) == 40);
        } catch (IndexOutOfBoundsException e) {
            assertTrue("Отсутствует запись rep2_statistics id = 4", row);
        }
    }

    public void assertTrueStatsRow5(List<Map<String, Object>> resultList, boolean row,
            int index) throws SQLException {
        try {
            Map<String, Object> result = resultList.get(index);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt((result.get(C_TYPE).toString())) == 1);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(ID_STRATEGY).toString()) == 6);
            assertTrue("Вставленные данные не совпадают", result.get(ID_TABLE).toString().equals(T_HOUSE));
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(C_COUNT).toString()) == 50);
        } catch (IndexOutOfBoundsException e) {
            assertTrue("Отсутствует запись rep2_statistics id = 5", row);
        }
    }

    public void assertTrueStatsRow6(List<Map<String, Object>> resultList, boolean row,
            int index) throws SQLException {
        try {
            Map<String, Object> result = resultList.get(index);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt((result.get(C_TYPE).toString())) == 0);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(ID_STRATEGY).toString()) == 7);
            assertTrue("Вставленные данные не совпадают", result.get(ID_TABLE).toString().equals(T_PLACE));
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(C_COUNT).toString()) == 60);
        } catch (IndexOutOfBoundsException e) {
            assertTrue("Отсутствует запись rep2_statistics id = 6", row);
        }
    }

    public void assertTrueStatsRow7(List<Map<String, Object>> resultList, boolean row,
            int index) throws SQLException {
        try {
            Map<String, Object> result = resultList.get(index);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt((result.get(C_TYPE).toString())) == 1);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(ID_STRATEGY).toString()) == 8);
            assertTrue("Вставленные данные не совпадают", result.get(ID_TABLE) .toString().equals(T_BASES));
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(C_COUNT).toString()) == 70);
        } catch (IndexOutOfBoundsException e) {
            assertTrue("Отсутствует запись rep2_statistics id = 7", row);
        }
    }

    public void assertTrueStatsRow8(List<Map<String, Object>> resultList, boolean row,
            int index) throws SQLException {
        try {
            Map<String, Object> result = resultList.get(index);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt((result.get(C_TYPE).toString())) == 0);
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(ID_STRATEGY).toString()) == 9);
            assertTrue("Вставленные данные не совпадают", result.get(ID_TABLE).toString().equals(T_HOUSE));
            assertTrue("Вставленные данные не совпадают", Integer.parseInt(result.get(C_COUNT).toString()) == 80);
        } catch (IndexOutOfBoundsException e) {
            assertTrue("Отсутствует запись rep2_statistics id = 8", row);
        }
    }
}
