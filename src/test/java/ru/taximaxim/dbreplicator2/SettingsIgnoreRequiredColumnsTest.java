package ru.taximaxim.dbreplicator2;

import static org.junit.Assert.assertEquals;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.hibernate.criterion.Restrictions;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractSettingTest;
import ru.taximaxim.dbreplicator2.model.IgnoreColumnsTableModel;
import ru.taximaxim.dbreplicator2.model.RequiredColumnsTableModel;
import ru.taximaxim.dbreplicator2.model.TableModel;
import ru.taximaxim.dbreplicator2.utils.Utils;

public class SettingsIgnoreRequiredColumnsTest extends AbstractSettingTest {

    protected static final Logger LOG = Logger.getLogger(SettingsIgnoreRequiredColumnsTest.class);

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
        Set<String> b = new HashSet <String>();
        b.add("A");
        b.add("B");
        b.add("C");
        b.add("D");
        b.add("E");
        b.add("F");
        b.add("G");
        
        Set<String> i = new HashSet <String>();
        i.add("C");
        i.add("E");
        i.add("G");
        
        Set<String> r = new HashSet <String>();
        r.add("B");
        r.add("D");
        r.add("C");
        
        for (String ignoredCol: i) {
            b.remove(ignoredCol.toUpperCase());
        }        
        
        LOG.info("==================================================");
        for (String baseCol: b) {
            boolean ignore = !baseCol.equals("C") & !baseCol.equals("E") & !baseCol.equals("G");
            assertEquals("Ошибка присудствует игнорируемая колонка", true, ignore);
            LOG.info("colmName: " + baseCol.toUpperCase());
        }
        LOG.info("==================================================");
        
        b.retainAll(r);
        
        LOG.info("==================================================");
        for (String baseCol: b) {
            boolean repl = baseCol.equals("B") | baseCol.equals("D");
            assertEquals("Ошибка присудствует колонка которая не включена в репликацию", true, repl);
            LOG.info("colmName: " + baseCol.toUpperCase());
        }
        LOG.info("==================================================");
    }
    
    @Test
    public void testalg2() {
        Set<String> b = new HashSet <String>();
        b.add("A");
        b.add("B");
        b.add("C");
        b.add("D");
        b.add("E");
        b.add("F");
        b.add("G");
        
        Set<String> i = new HashSet <String>();
        i.add("C");
        i.add("E");
        i.add("G");
        
        Set<String> r = new HashSet <String>();
        
        for (String ignoredCol: i) {
            b.remove(ignoredCol.toUpperCase());
        }
        
        LOG.info("==================================================");
        for (String baseCol: b) {
            boolean ignore = !baseCol.equals("C") & !baseCol.equals("E") & !baseCol.equals("G");
            assertEquals("Ошибка присудствует игнорируемая колонка", true, ignore);
            LOG.info("colmName: " + baseCol.toUpperCase());
        }
        LOG.info("==================================================");
        
        b.retainAll(r);
        
        LOG.info("==================================================");
        for (String baseCol: b) {
            boolean repl = baseCol.equals("B") | baseCol.equals("D") | baseCol.equals("A") | baseCol.equals("F");
            assertEquals("Ошибка присудствует колонка которая не включена в репликацию", true, repl);
            LOG.info("colmName: " + baseCol.toUpperCase());
        }
        LOG.info("==================================================");
    }
    
    @Test
    public void testalg3() {
        Set<String> b = new HashSet <String>();
        b.add("A");
        b.add("B");
        b.add("C");
        b.add("D");
        b.add("E");
        b.add("F");
        b.add("G");
        
        Set<String> i = new HashSet <String>();
        i.add("C");
        i.add("E");
        i.add("G");
        
        Set<String> r = new HashSet <String>();
        r.add("A");
        r.add("B");
        r.add("C");
        r.add("D");
        r.add("E");
        r.add("F");
        r.add("G");
        r.add("M");
        
        for (String ignoredCol: i) {
            b.remove(ignoredCol.toUpperCase());
        }
        
        LOG.info("==================================================");
        for (String baseCol: b) {
            boolean ignore = !baseCol.equals("C") & !baseCol.equals("E") & !baseCol.equals("G");
            assertEquals("Ошибка присудствует игнорируемая колонка", true, ignore);
            LOG.info("colmName: " + baseCol.toUpperCase());
        }
        LOG.info("==================================================");
        
        b.retainAll(r);
        
        LOG.info("==================================================");
        for (String baseCol: b) {
            boolean repl = baseCol.equals("B") | baseCol.equals("D") | baseCol.equals("F") | baseCol.equals("A");
            assertEquals("Ошибка присудствует колонка которая не включена в репликацию", true, repl);
            LOG.info("colmName: " + baseCol.toUpperCase());
        }
        LOG.info("==================================================");
    }
    
    @Test
    public void testRequiredColumns() {
        
        TableModel table = (TableModel) session.get(TableModel.class, 2);

        if (table.getRequiredColumnsTable() != null) {
            for (RequiredColumnsTableModel requiredColumn : table.getRequiredColumnsTable()) {
                LOG.info("getId:         " + requiredColumn.getId());
                LOG.info("getColumnName: " + requiredColumn.getColumnName());
                LOG.info("getTable:      " + requiredColumn.getTable().getName());
                LOG.info("===========================================================");
                
                boolean repl = requiredColumn.getColumnName().equals("ID") |  
                        requiredColumn.getColumnName().equals("_INT") | 
                        requiredColumn.getColumnName().equals("_BOOLEAN") |
                        requiredColumn.getColumnName().equals("_LONG") |
                        requiredColumn.getColumnName().equals("_DECIMAL") |
                        requiredColumn.getColumnName().equals("_DOUBLE") |
                        requiredColumn.getColumnName().equals("_FLOAT") |
                        requiredColumn.getColumnName().equals("_BYTE") |
                        requiredColumn.getColumnName().equals("_DATE") |
                        requiredColumn.getColumnName().equals("_TIME") |
                        requiredColumn.getColumnName().equals("_TIMESTAMP") |
                        requiredColumn.getColumnName().equals("_NOCOLOMN") |
                        requiredColumn.getColumnName().equals("_STRING");
                
                assertEquals("Ошибка в реплицируеемых колонках " + requiredColumn.getColumnName(), true,  repl);
                
                assertEquals("Ошибка Название таблиц не равны!", table.getName(),  requiredColumn.getTable().getName());
            }

            List<TableModel> tableList = Utils.castList(
                    TableModel.class,
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
    public void testIgnoreColumns() {

        TableModel table = (TableModel) session.get(TableModel.class, 2);

        if (table.getIgnoreColumnsTable() != null) {
            for (IgnoreColumnsTableModel ignoredColumn : table.getIgnoreColumnsTable()) {
                LOG.info("getId:         " + ignoredColumn.getId());
                LOG.info("getColumnName: " + ignoredColumn.getColumnName());
                LOG.info("getTable:      " + ignoredColumn.getTable().getName());
                LOG.info("===========================================================");
                assertEquals("Ошибка Название игнорируеммой колонки не верно!", ignoredColumn.getColumnName(),
                        "_STRING");
                assertEquals("Ошибка Название таблиц не равны!", table.getName(),
                        ignoredColumn.getTable().getName());
            }

            List<TableModel> tableList = Utils.castList(
                    TableModel.class,
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
}
