package ru.taximaxim.dbreplicator2;

import static org.junit.Assert.*;

import java.util.List;

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
import ru.taximaxim.dbreplicator2.model.IgnoreColumnsTableService;
import ru.taximaxim.dbreplicator2.utils.Core;

public class test {
    
    protected static final Logger LOG = Logger.getLogger(test.class);
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
    public void tester() {

        sessionFactory = Core.getSessionFactory();
        session = sessionFactory.openSession();
        connectionFactory = Core.getConnectionFactory();
        
        LOG.info("log");
        IgnoreColumnsTableService ignore = new IgnoreColumnsTableService(sessionFactory);
        List<IgnoreColumnsTableModel> tab = ignore.getIgnoreList(2);
        
        LOG.info(tab.size());
        
        for (IgnoreColumnsTableModel tableModel : tab) {
            LOG.info(tableModel.getColumnName());
        }
    }

}
