package ru.taximaxim.dbreplicator2;

import static org.junit.Assert.assertEquals;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.model.IgnoreColumnsTableModel;
import ru.taximaxim.dbreplicator2.model.TableModel;
import ru.taximaxim.dbreplicator2.utils.Core;

public class IgnoreColumnsTableModelTest {
    
    protected static final Logger LOG = Logger.getLogger(IgnoreColumnsTableModelTest.class);
    protected static SessionFactory sessionFactory;
    protected static Session session;
    protected static ConnectionFactory connectionFactory;
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void test() {

        sessionFactory = Core.getSessionFactory();
        session = sessionFactory.openSession();
        connectionFactory = Core.getConnectionFactory();
        
        TableModel table = (TableModel) session.get(TableModel.class, 2);
        for (IgnoreColumnsTableModel ignoredColumn : table.getIgnoreColumnsTable()) {
            LOG.info("getId:         " + ignoredColumn.getId());
            LOG.info("getColumnName: " + ignoredColumn.getColumnName());
            LOG.info("getTable:      " + ignoredColumn.getTable().getName());

            assertEquals("Ошибка Название таблиц не равны!", table.getName(), ignoredColumn.getTable().getName()); 
        }
    }

}
