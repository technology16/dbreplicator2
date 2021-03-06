package ru.taximaxim.dbreplicator2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.hibernate.criterion.Restrictions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractSettingTest;
import ru.taximaxim.dbreplicator2.model.TableModel;
import ru.taximaxim.dbreplicator2.utils.Utils;

public class TableModelTest extends AbstractSettingTest {

    protected static final Logger LOG = Logger
            .getLogger(TableModelTest.class);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp(null, null, null);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        close();
    }

    @Test
    public void testalg() {
        Set<String> b = new HashSet<String>();
        b.add("A");
        b.add("B");
        b.add("C");
        b.add("D");
        b.add("E");
        b.add("F");
        b.add("G");

        Set<String> i = new HashSet<String>();
        i.add("C");
        i.add("E");
        i.add("G");

        Set<String> r = new HashSet<String>();
        r.add("B");
        r.add("D");
        r.add("C");

        for (String ignoredCol : i) {
            b.remove(ignoredCol.toUpperCase());
        }

        LOG.info("==================================================");
        for (String baseCol : b) {
            boolean ignore = !baseCol.equals("C") & !baseCol.equals("E")
                    & !baseCol.equals("G");
            assertEquals("Ошибка присудствует игнорируемая колонка", true, ignore);
            LOG.info("colmName: " + baseCol.toUpperCase());
        }
        LOG.info("==================================================");

        b.retainAll(r);

        LOG.info("==================================================");
        for (String baseCol : b) {
            boolean repl = baseCol.equals("B") | baseCol.equals("D");
            assertEquals("Ошибка присудствует колонка которая не включена в репликацию",
                    true, repl);
            LOG.info("colmName: " + baseCol.toUpperCase());
        }
        LOG.info("==================================================");
    }

    @Test
    public void testalg2() {
        Set<String> b = new HashSet<String>();
        b.add("A");
        b.add("B");
        b.add("C");
        b.add("D");
        b.add("E");
        b.add("F");
        b.add("G");

        Set<String> i = new HashSet<String>();
        i.add("C");
        i.add("E");
        i.add("G");

        Set<String> r = new HashSet<String>();

        for (String ignoredCol : i) {
            b.remove(ignoredCol.toUpperCase());
        }

        LOG.info("==================================================");
        for (String baseCol : b) {
            boolean ignore = !baseCol.equals("C") & !baseCol.equals("E")
                    & !baseCol.equals("G");
            assertEquals("Ошибка присудствует игнорируемая колонка", true, ignore);
            LOG.info("colmName: " + baseCol.toUpperCase());
        }
        LOG.info("==================================================");

        b.retainAll(r);

        LOG.info("==================================================");
        for (String baseCol : b) {
            boolean repl = baseCol.equals("B") | baseCol.equals("D") | baseCol.equals("A")
                    | baseCol.equals("F");
            assertEquals("Ошибка присудствует колонка которая не включена в репликацию",
                    true, repl);
            LOG.info("colmName: " + baseCol.toUpperCase());
        }
        LOG.info("==================================================");
    }

    @Test
    public void testalg3() {
        Set<String> b = new HashSet<String>();
        b.add("A");
        b.add("B");
        b.add("C");
        b.add("D");
        b.add("E");
        b.add("F");
        b.add("G");

        Set<String> i = new HashSet<String>();
        i.add("C");
        i.add("E");
        i.add("G");

        Set<String> r = new HashSet<String>();
        r.add("A");
        r.add("B");
        r.add("C");
        r.add("D");
        r.add("E");
        r.add("F");
        r.add("G");
        r.add("M");

        for (String ignoredCol : i) {
            b.remove(ignoredCol.toUpperCase());
        }

        LOG.info("==================================================");
        for (String baseCol : b) {
            boolean ignore = !baseCol.equals("C") & !baseCol.equals("E")
                    & !baseCol.equals("G");
            assertEquals("Ошибка присудствует игнорируемая колонка", true, ignore);
            LOG.info("colmName: " + baseCol.toUpperCase());
        }
        LOG.info("==================================================");

        b.retainAll(r);

        LOG.info("==================================================");
        for (String baseCol : b) {
            boolean repl = baseCol.equals("B") | baseCol.equals("D") | baseCol.equals("F")
                    | baseCol.equals("A");
            assertEquals("Ошибка присудствует колонка которая не включена в репликацию",
                    true, repl);
            LOG.info("colmName: " + baseCol.toUpperCase());
        }
        LOG.info("==================================================");
    }

    @Test
    public void testRequiredColumns() {

        TableModel table = (TableModel) session.get(TableModel.class, 2);

        if (table.getRequiredColumns() != null) {
            for (String requiredColumn : table.getRequiredColumns()) {
                LOG.info("requiredColumnName: " + requiredColumn);
                LOG.info("===========================================================");

                boolean repl = requiredColumn.equals("ID") | requiredColumn.equals("_INT")
                        | requiredColumn.equals("_BOOLEAN")
                        | requiredColumn.equals("_LONG")
                        | requiredColumn.equals("_DECIMAL")
                        | requiredColumn.equals("_DOUBLE")
                        | requiredColumn.equals("_FLOAT") | requiredColumn.equals("_BYTE")
                        | requiredColumn.equals("_DATE") | requiredColumn.equals("_TIME")
                        | requiredColumn.equals("_TIMESTAMP")
                        | requiredColumn.equals("_NOCOLOMN")
                        | requiredColumn.equals("_STRING");

                assertEquals("Ошибка в реплицируеемых колонках " + requiredColumn, true,
                        repl);
            }

            List<TableModel> tableList = Utils.castList(TableModel.class,
                    session.createCriteria(TableModel.class, "tables")
                            .add(Restrictions.eq("name", "t_table1")).list());
            LOG.info("LOG:");
            for (TableModel tableModel : tableList) {
                LOG.info("tableModel: " + tableModel.getTableId());
                LOG.info("tableModel: " + tableModel.getName());
            }
        } else {
            fail("Список реплицируеммых колонок пуст ");
        }
    }

    @Test
    public void testIgnoreColumns() {

        TableModel table = (TableModel) session.get(TableModel.class, 2);

        if (table.getIgnoredColumns() != null) {
            for (String ignoredColumn : table.getIgnoredColumns()) {
                LOG.info("ignoredColumnName: " + ignoredColumn);
                LOG.info("===========================================================");
                assertEquals("Ошибка Название игнорируеммой колонки не верно!",
                        "_STRING", ignoredColumn);
            }

            List<TableModel> tableList = Utils.castList(TableModel.class,
                    session.createCriteria(TableModel.class, "tables")
                            .add(Restrictions.eq("name", "t_table1")).list());
            LOG.info("LOG:");
            for (TableModel tableModel : tableList) {
                LOG.info("tableModel: " + tableModel.getTableId());
                LOG.info("tableModel: " + tableModel.getName());
            }
        } else {
            fail("Список игнорируеммых колонок пуст ");
        }
    }

    /**
     * Тестируем корректность копирования таблицы
     */
    @Test
    public void testCopyTable() {
        TableModel table = (TableModel) session.get(TableModel.class, 2);

        // Проверяем таблицу перед копированием
        checkTable(table);
        
        // Клонируем таблицу
        TableModel copy = table.copy();
        copy.setName("clone_" + table.getName());
        copy.setRunner(null);
        copy.setParam(TableModel.REQUIRED_COLUMNS, "");
        copy.setParam(TableModel.IGNORED_COLUMNS, "");

        // Проверяем таблицу после копирования
        checkTable(table);

        // Проверяем корректность копии
        assertEquals("У копии не верное имя!", "clone_" + table.getName(),
                copy.getName());
        assertEquals("У копии остались игнорируемые колонки!", 0,
                copy.getIgnoredColumns().size());
        assertEquals("У копии остались обязательные колонки!", 0,
                copy.getRequiredColumns().size());
    }

    /**
     * Проверка корректности настроек
     * 
     * @param table
     */
    protected void checkTable(TableModel table) {
        assertEquals("У таблицы " + table.getName() + " не верное имя!", 
                "T_TABLE1", table.getName());
        for (String ignoredColumn : table.getIgnoredColumns()) {
            assertEquals(
                    "Настройках таблицы" + table.getName()
                            + " не верное имя игнорируемой колонки!",
                    "_STRING", ignoredColumn);
        }

        assertNotEquals("У таблицы " + table.getName() + " пропали обязательные колонки!",
                0, table.getIgnoredColumns().size());
        assertNotEquals("У таблицы " + table.getName() + " пропали обязательные колонки!",
                0, table.getIgnoredColumns().size());
    }
}
