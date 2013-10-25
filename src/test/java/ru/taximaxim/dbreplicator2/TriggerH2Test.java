package ru.taximaxim.dbreplicator2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.h2.api.Trigger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.cf.BoneCPConnectionsFactory;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsImpl;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsService;

public class TriggerH2Test {
    
    protected static final Logger LOG = Logger.getLogger(H2CreateSetting.class);
    protected static SessionFactory sessionFactory;
    protected static Session session;
    
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
    public void test() throws SQLException, ClassNotFoundException {
          
        Class.forName("org.h2.Driver");
        Connection conn = getConnection(); //DriverManager.getConnection("jdbc:h2:mem://localhost/~/test", "sa", "");
        
        Statement stat = conn.createStatement();
        
        delete(stat, "rep2_superlog");
        delete(stat, "rep2_workpool_data");
        
        delete(stat, "T_TABLE1");
        delete(stat, "T_TABLE2");
        delete(stat, "T_TABLE3");
        delete(stat, "T_TABLE4");
        delete(stat, "T_TABLE5");
        delete(stat, "T_TABLE6");
        delete(stat, "T_TABLE7");
        delete(stat, "T_TABLE8");
        delete(stat, "T_TABLE9");
        delete(stat, "T_TABLE0");
        
        
        stat.execute("CREATE TABLE rep2_superlog(id_superlog IDENTITY PRIMARY KEY, id_foreign INTEGER, id_table NVARCHAR, c_operation VARCHAR(1), c_date TIMESTAMP, id_transaction NVARCHAR);");
        stat.execute("CREATE TABLE rep2_workpool_data(id_runner INTEGER, id_superlog BIGINT, id_foreign INTEGER, id_table NVARCHAR, c_operation VARCHAR(1), c_date TIMESTAMP, id_transaction NVARCHAR);");
        
        
        stat.execute("CREATE TABLE T_TABLE1(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        createTrigger(stat, "T_TABLE1");
        
        stat.execute("CREATE TABLE T_TABLE2(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        createTrigger(stat, "T_TABLE2");
        
        stat.execute("CREATE TABLE T_TABLE3(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        createTrigger(stat, "T_TABLE3");
        
        stat.execute("CREATE TABLE T_TABLE4(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        createTrigger(stat, "T_TABLE4");
        
        stat.execute("CREATE TABLE T_TABLE5(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        createTrigger(stat, "T_TABLE5");
        
        stat.execute("CREATE TABLE T_TABLE6(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        createTrigger(stat, "T_TABLE6");
        
        stat.execute("CREATE TABLE T_TABLE7(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        createTrigger(stat, "T_TABLE7");
        
        stat.execute("CREATE TABLE T_TABLE8(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        createTrigger(stat, "T_TABLE8");
        
        stat.execute("CREATE TABLE T_TABLE9(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        createTrigger(stat, "T_TABLE9");
        
        stat.execute("CREATE TABLE T_TABLE0(ID INT PRIMARY KEY, C_AMOUNT DECIMAL, C_NAME VARCHAR(50))");
        createTrigger(stat, "T_TABLE0");     
        
        try {
            
            stat.execute("INSERT INTO T_TABLE1 VALUES(1, 10.00, 'TESTER')");
            stat.execute("INSERT INTO T_TABLE1 VALUES(2, 19.95, 'TESTER')");
            stat.execute("UPDATE T_TABLE1 SET C_AMOUNT = 20.0 WHERE ID = 2");
            stat.execute("DELETE FROM T_TABLE1 WHERE ID = 1");
            
            stat.execute("INSERT INTO T_TABLE2 VALUES(1, 14.50, 'TESTER')");
            stat.execute("INSERT INTO T_TABLE2 VALUES(2, 12.55, 'TESTER')");
            stat.execute("UPDATE T_TABLE2 SET C_AMOUNT = 60.0 WHERE ID = 2");
            stat.execute("DELETE FROM T_TABLE2 WHERE ID = 1");
            
            stat.execute("INSERT INTO T_TABLE3 VALUES(1, 05.00, 'TESTER')");
            stat.execute("INSERT INTO T_TABLE3 VALUES(2, 78.55, 'TESTER')");
            stat.execute("UPDATE T_TABLE3 SET C_AMOUNT = 67.99 WHERE ID = 2");
            stat.execute("DELETE FROM T_TABLE3 WHERE ID = 1");
            
            stat.execute("INSERT INTO T_TABLE4 VALUES(1, 37.00, 'TESTER')");
            stat.execute("INSERT INTO T_TABLE4 VALUES(2, 13.88, 'TESTER')");
            stat.execute("UPDATE T_TABLE4 SET C_AMOUNT = 23.78 WHERE ID = 2");
            stat.execute("DELETE FROM T_TABLE4 WHERE ID = 1");
            
            stat.execute("INSERT INTO T_TABLE5 VALUES(1, 86.00, 'TESTER')");
            stat.execute("INSERT INTO T_TABLE5 VALUES(2, 99.99, 'TESTER')");
            stat.execute("UPDATE T_TABLE5 SET C_AMOUNT = 10.09 WHERE ID = 2");
            stat.execute("DELETE FROM T_TABLE5 WHERE ID = 1");
            
            stat.execute("INSERT INTO T_TABLE6 VALUES(1, 44.55, 'TESTER')");
            stat.execute("INSERT INTO T_TABLE6 VALUES(2, 36.47, 'TESTER')");
            stat.execute("UPDATE T_TABLE6 SET C_AMOUNT = 79.80 WHERE ID = 2");
            stat.execute("DELETE FROM T_TABLE6 WHERE ID = 1");
            
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
        ResultSet rs;
        rs = stat.executeQuery("select id_superlog, id_foreign, id_table, c_operation, c_date, id_transaction from rep2_superlog");
        ResultSetMetaData rData = rs.getMetaData();
        int totalColumn = rData.getColumnCount();
        
        while (rs.next()) {
            for (int i = 1; i <= totalColumn; i++) {
                //LOG.info(rs.getObject(i));
            }
            //LOG.info("=====================");
        }
        conn.close();
    }
    
    public Connection getConnection() throws ClassNotFoundException, SQLException{
        sessionFactory = Application.getSessionFactory();
        session = sessionFactory.openSession();
        String poolName = "pull";
        BoneCPSettingsService cpSettingsService = new BoneCPSettingsService(sessionFactory);

        BoneCPSettingsImpl settingsPool = new BoneCPSettingsImpl(
                poolName, "org.h2.Driver", "jdbc:h2:mem://localhost/~/test", "sa", "");
        cpSettingsService.setDataBaseSettings(settingsPool);

        BoneCPConnectionsFactory connectionsFactory = new BoneCPConnectionsFactory(cpSettingsService);
        Connection conn = null;
        try {
            conn = connectionsFactory.getConnection(poolName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw e;
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
        return conn;
    }
    
    
    public void delete(Statement stat, String tableName) throws SQLException{
        
        stat.execute("Drop Table if exists " + tableName);
    }

    
    public void createTrigger(Statement stat, String tableName) throws SQLException{

        String Trigger = TriggerH2Test.MyTrigger.class.getName();
        stat.execute("CREATE TRIGGER " + tableName + "_INS AFTER INSERT ON " + tableName + " FOR EACH ROW CALL \""+Trigger+"\"");
        stat.execute("CREATE TRIGGER " + tableName + "_UPD AFTER UPDATE ON " + tableName + " FOR EACH ROW CALL \""+Trigger+"\"");
        stat.execute("CREATE TRIGGER " + tableName + "_DEL AFTER DELETE ON " + tableName + " FOR EACH ROW CALL \""+Trigger+"\"");  
    }
    
    public static class MyTrigger implements Trigger {

        String tableName = null;
        int type = -1;
        
        public void init(Connection conn, String schemaName, String triggerName, String tableName, boolean before, int type) {
            // Initializing trigger
            this.tableName = tableName;
            this.type = type;
        }
        
        public void fire(Connection conn, Object[] oldRow, Object[] newRow)
                throws SQLException {
            
            String operation = null;
            Object id_foreign = null; 
            
            if(type == 2) {
                operation = "U";
                id_foreign = newRow[0];
            } else if (type == 1) {
                operation = "I";
                id_foreign = newRow[0];
            } else if (type == 4) {
                operation = "D";
                id_foreign = oldRow[0];
            }
            
//            if (newRow != null & oldRow != null) {
//                operation = "U";
//                if(newRow[0] == oldRow[0]) {
//                    id_foreign = newRow[0];
//                }
//            } else if (newRow != null) {
//                operation = "I";
//                id_foreign = newRow[0]; 
//            } else if (oldRow != null) {
//                operation = "D";
//                id_foreign = oldRow[0];
//            }
            
            PreparedStatement prep = 
                conn.prepareStatement("INSERT INTO rep2_superlog " +
                    "(id_foreign, id_table, c_operation, c_date, id_transaction)" +
                    " VALUES(?, ?, ?, now(), ?)");
            prep.setObject(1, id_foreign);
            prep.setObject(2, tableName);
            prep.setObject(3, operation);
            prep.setObject(4, 0);
            prep.execute();
        }

        @Override
        public void close() throws SQLException {
            
        }

        @Override
        public void remove() throws SQLException {
            
        }
    }
   
}
