package ru.taximaxim.dbreplicator2.abstracts;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.hibernate.SessionFactory;
import org.hibernate.Session;

import ru.taximaxim.dbreplicator2.Helper;
import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.utils.Core;

public abstract class AbstractTest {
    
    protected static final Logger LOG = Logger.getLogger(AbstractTest.class);
    
    protected static SessionFactory sessionFactory;
    protected static Session session;
    protected static ConnectionFactory connectionFactory;
    protected static Connection conn = null;
    protected static Connection connDest = null;
    protected static Runnable worker = null;
    protected static Runnable workerPg = null;
    protected static Runnable workerMs = null;
    protected static Runnable errorsCountWatchdogWorker = null;

    protected static void setUp(String xmlForConfig, String xmlForSession, String sqlRep2, String sqlSourse, String sqlDest) throws ClassNotFoundException, SQLException, IOException {
        
        Core.configurationClose();
        Core.getConfiguration(xmlForConfig);
        
        if(xmlForSession != null){
            sessionFactory = Core.getSessionFactory(xmlForSession);
        }
        else {
            sessionFactory = Core.getSessionFactory();
        }
       
        session = sessionFactory.openSession();
        connectionFactory = Core.getConnectionFactory();
        initialization(sqlRep2, sqlSourse, sqlDest);
    
    }
    
    /**
     * Закрытие соединений
     * @throws SQLException 
     * @throws InterruptedException 
     */
    protected static void close() throws SQLException, InterruptedException {
        if(conn!=null)
            conn.close();
        if(connDest!=null)
            connDest.close();
        if(session!=null)
            session.close();
        sessionFactory.close();
        Core.connectionFactoryClose();
        Core.sessionFactoryClose();
        Core.statsServiceClose();
        Core.tasksPoolClose();
        Core.taskSettingsServiceClose();
        Core.configurationClose();
        Core.threadPoolClose();
     
    }
    
    /**
     * Инициализация
     */
    public static void initialization(String sqlRep2, String sqlSourse, String sqlDest) throws ClassNotFoundException, SQLException, IOException{
        LOG.info("initialization");
        String source = "source";
        conn = connectionFactory.getConnection(source);

        Helper.executeSqlFromFile(conn, sqlRep2);
        Helper.executeSqlFromFile(conn, sqlSourse);

        String dest = "dest";
        connDest = connectionFactory.getConnection(dest);
        Helper.executeSqlFromFile(connDest, sqlRep2);
        Helper.executeSqlFromFile(connDest, sqlDest);
    }

    
}

