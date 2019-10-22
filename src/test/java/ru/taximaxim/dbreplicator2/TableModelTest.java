package ru.taximaxim.dbreplicator2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.hibernate.HibernateException;
import org.hibernate.criterion.Restrictions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractSettingTest;
import ru.taximaxim.dbreplicator2.el.FatalReplicationException;
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
    public void testRequiredColumns() throws HibernateException, FatalReplicationException {

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
            assertEquals("Список реплицируеммых колонок пуст ", false);
        }
    }

    @Test
    public void testIgnoreColumns() throws HibernateException, FatalReplicationException {

        TableModel table = (TableModel) session.get(TableModel.class, 2);

        if (table.getIgnoredColumns() != null) {
            for (String ignoredColumn : table.getIgnoredColumns()) {
                LOG.info("ignoredColumnName: " + ignoredColumn);
                LOG.info("===========================================================");
                assertEquals("Ошибка Название игнорируеммой колонки не верно!",
                        ignoredColumn, "_STRING");
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
            assertEquals("Список игнорируеммых колонок пуст ", false);
        }
    }

    /**
     * Тестируем корректность клонирования таблицы
     * 
     * @throws CloneNotSupportedException
     * @throws FatalReplicationException 
     */
    @Test
    public void testCloneTable() throws CloneNotSupportedException, FatalReplicationException {
        TableModel table = (TableModel) session.get(TableModel.class, 2);

        // Проверяем таблицу перед клонированием
        checkTable(table);
        
        // Клонируем таблицу
        TableModel clone = (TableModel) table.clone();
        clone.setName("clone_" + table.getName());
        clone.setRunner(null);
        clone.setParam(TableModel.REQUIRED_COLUMNS, "");
        clone.setParam(TableModel.IGNORED_COLUMNS, "");

        // Проверяем таблицу после клонированием
        checkTable(table);

        // Проверяем корректность клона
        assertEquals("У клона не верное имя!", "clone_" + table.getName(),
                clone.getName());
        assertEquals("У клона остались игнорируемые колонки!", 0,
                clone.getIgnoredColumns().size());
        assertEquals("У клона остались обязательные колонки!", 0,
                clone.getRequiredColumns().size());
    }

    /**
     * Проверка корректности настроек
     * 
     * @param table
     * @throws FatalReplicationException 
     */
    protected void checkTable(TableModel table) throws FatalReplicationException {
        assertEquals("У таблицы " + table.getName() + " не верное имя!", 
                "T_TABLE1", table.getName());
        for (String ignoredColumn : table.getIgnoredColumns()) {
            assertEquals(
                    "Настройках таблицы" + table.getName()
                            + " не верное имя игнорируемой колонки!",
                    ignoredColumn, "_STRING");
        }

        assertNotEquals("У таблицы " + table.getName() + " пропали обязательные колонки!",
                0, table.getIgnoredColumns().size());
        assertNotEquals("У таблицы " + table.getName() + " пропали обязательные колонки!",
                0, table.getIgnoredColumns().size());
    }
}
