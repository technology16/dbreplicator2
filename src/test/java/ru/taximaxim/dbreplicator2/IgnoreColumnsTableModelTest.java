package ru.taximaxim.dbreplicator2;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.model.IgnoreColumnsTableModel;
import ru.taximaxim.dbreplicator2.model.TableModel;
import ru.taximaxim.dbreplicator2.utils.Core;
import ru.taximaxim.dbreplicator2.utils.Utils;

public class IgnoreColumnsTableModelTest {

    protected static final Logger LOG = Logger
            .getLogger(IgnoreColumnsTableModelTest.class);
    protected static SessionFactory sessionFactory;
    protected static Session session;
    protected static ConnectionFactory connectionFactory;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        sessionFactory = Core.getSessionFactory();
        session = sessionFactory.openSession();
        connectionFactory = Core.getConnectionFactory();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        session.close();
        Core.sessionFactoryClose();
        Core.connectionFactoryClose();
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {

        TableModel table = (TableModel) session.get(TableModel.class, 2);
        if (table.getIgnoreColumnsTable() != null) {
            for (IgnoreColumnsTableModel ignoredColumn : table.getIgnoreColumnsTable()) {
                LOG.info("getId:         " + ignoredColumn.getId());
                LOG.info("getColumnName: " + ignoredColumn.getColumnName());
                LOG.info("getTable:      " + ignoredColumn.getTable().getName());

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
        }
    }
}
