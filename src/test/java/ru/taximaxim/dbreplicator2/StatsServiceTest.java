/**
 *
 */
package ru.taximaxim.dbreplicator2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractSettingTest;
import ru.taximaxim.dbreplicator2.stats.StatsService;

/**
 * @author mardanov_rm
 *
 */
public class StatsServiceTest extends AbstractSettingTest {
    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 100;
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
        setUp("init_db/importRep2.sql", null, null);
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
    public static void initialization() throws SQLException, InterruptedException {
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
    public void testGetStatByTypeTablePeriod() throws SQLException {
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
    public void testgetStatByType() throws SQLException {
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
    public void testgetStatByTypeTable() throws SQLException {
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
    public void testgetStatByTypePeriod() throws SQLException {
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
    public void testgetStatByTypeTablePeriodStartMidde() throws SQLException {
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
    public void testgetStatByTypeTablePeriodMiddeEnd() throws SQLException {
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

    private void assertTrueStatsRow1(List<Map<String, Object>> resultList, boolean row, int index) {
        assertTrueStatsRow(resultList, row, index, 1, 2, T_PLACE, 10, 1);
    }

    private void assertTrueStatsRow2(List<Map<String, Object>> resultList, boolean row, int index) {
        assertTrueStatsRow(resultList, row, index, 0, 3, T_BASES, 20, 2);
    }

    private void assertTrueStatsRow3(List<Map<String, Object>> resultList, boolean row, int index) {
        assertTrueStatsRow(resultList, row, index, 1, 4, T_HOUSE, 30, 3);
    }

    private void assertTrueStatsRow4(List<Map<String, Object>> resultList, boolean row, int index) {
        assertTrueStatsRow(resultList, row, index, 0, 5, T_PLACE, 40, 4);
    }

    private void assertTrueStatsRow5(List<Map<String, Object>> resultList, boolean row, int index) {
        assertTrueStatsRow(resultList, row, index, 1, 6, T_HOUSE, 50, 5);
    }

    private void assertTrueStatsRow6(List<Map<String, Object>> resultList, boolean row, int index) {
        assertTrueStatsRow(resultList, row, index, 0, 7, T_PLACE, 60, 6);
    }

    private void assertTrueStatsRow7(List<Map<String, Object>> resultList, boolean row, int index) {
        assertTrueStatsRow(resultList, row, index, 1, 8, T_BASES, 70, 7);
    }

    private void assertTrueStatsRow8(List<Map<String, Object>> resultList, boolean row, int index) {
        assertTrueStatsRow(resultList, row, index, 0, 9, T_HOUSE, 80, 8);
    }

    private void assertTrueStatsRow(List<Map<String, Object>> resultList, boolean row, int index,
            int cType, int idStrategy, String tableName, int cCount, int idStatistics) {
        try {
            Map<String, Object> result = resultList.get(index);
            assertEquals("Вставленные данные не совпадают", cType, Integer.parseInt((result.get(C_TYPE).toString())));
            assertEquals("Вставленные данные не совпадают", idStrategy, Integer.parseInt(result.get(ID_STRATEGY).toString()));
            assertEquals("Вставленные данные не совпадают", tableName, result.get(ID_TABLE).toString());
            assertEquals("Вставленные данные не совпадают", cCount, Integer.parseInt(result.get(C_COUNT).toString()));
        } catch (IndexOutOfBoundsException e) {
            assertTrue("Отсутствует запись rep2_statistics id = " + idStatistics, row);
        }
    }
}
