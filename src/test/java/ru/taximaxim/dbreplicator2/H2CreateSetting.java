/* 
 * The MIT License (MIT)
 * 
 * Copyright (c) 2013 Technologiya
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package ru.taximaxim.dbreplicator2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.h2.api.Trigger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.cf.BoneCPConnectionsFactory;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsModel;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsService;
import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.model.RunnerService;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.model.TaskSettingsModel;

/**
 * Тест соединения с базой данных по протоколу TCP.
 * 
 * @author ags
 */
public class H2CreateSetting {

    protected static final Logger LOG = Logger.getLogger(H2CreateSetting.class);
    protected static SessionFactory sessionFactory;
    protected static Session session;
    protected static Statement stat;
    protected static Connection conn = null;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        sessionFactory = Application.getSessionFactory();
        session = sessionFactory.openSession();
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        
    }

    @Test
    public void testConnection() throws SQLException, ClassNotFoundException {

        LOG.debug("Start: ");
        String target = "pull";
        String source = "source";
        String className = "ru.taximaxim.dbreplicator2.replica.SuperlogRunner";

        BoneCPSettingsService cpSettingsService = new BoneCPSettingsService(
                sessionFactory);

        BoneCPSettingsModel settingsTarget = new BoneCPSettingsModel(target,
                "org.h2.Driver", "jdbc:h2:mem://localhost/~/target", "sa", "");
        cpSettingsService.setDataBaseSettings(settingsTarget);
        BoneCPSettingsModel settingsSource = new BoneCPSettingsModel(source,
                "org.h2.Driver", "jdbc:h2:mem://localhost/~/source", "sa", "");
        cpSettingsService.setDataBaseSettings(settingsSource);

//        RunnerService runnerService = 
//                new RunnerService(Application.getSessionFactory());
        
        CreateTeble(cpSettingsService, source);

        LOG.info("<======Inception======>");
        ResultSet rsS = stat
                .executeQuery("select id_superlog, id_foreign, id_table, c_operation, c_date, id_transaction from rep2_superlog");
        ResultSetMetaData rDataS = rsS.getMetaData();
        int totalColumnS = rDataS.getColumnCount();
        while (rsS.next()) {
            String text = "";
            for (int i = 1; i <= totalColumnS; i++) {
                text += rsS.getObject(i).toString() + "\t";
            }
            LOG.info(text);
            LOG.info("================================================================");
        }
        LOG.info(">======Inception======<");

        RunnerModel runnerModel = createRunner(className, source, source, "description");
        StrategyModel strategyModel = createStrategy(
                "ru.taximaxim.dbreplicator2.replica.strategies.SuperLogManagerStrategy",
                null, true, 100);

        runnerModel.getStrategyModels().add(strategyModel);
        strategyModel.setRunner(runnerModel);

        session.saveOrUpdate(settingsSource);
        session.saveOrUpdate(settingsTarget);
        
        createRunnerModel(source, target);
        
        Runnable worker = new WorkerThread(runnerModel);
        worker.run();
        
        LOG.info("<====== RESULT ======>");
        ResultSet rs = stat
            .executeQuery("select id_superlog, id_foreign, id_table, c_operation, c_date, id_transaction from rep2_superlog");
        ResultSetMetaData rData = rs.getMetaData();
        int totalColumn = rData.getColumnCount();
        while (rs.next()) {
            String text = "";
            for (int i = 1; i <= totalColumn; i++) {
                text += rs.getObject(i).toString() + "\t";
            }
            LOG.info(text);
            LOG.info("=====================");
        }
        
        LOG.info("======= RESULT =======");
        
        ResultSet rsQ = stat
                .executeQuery("select id_runner, id_superlog, id_foreign, id_table, c_operation, c_date, id_transaction from rep2_workpool_data");
        ResultSetMetaData rDataQ = rsQ.getMetaData();
        int totalColumnQ = rDataQ.getColumnCount();

        while (rsQ.next()) {
            String text = "";
            for (int i = 1; i <= totalColumnQ; i++) {
                text += rsQ.getObject(i).toString() + "\t";
            }
            LOG.info(text);
            LOG.info("=====================");
        }
        LOG.info(">====== RESULT ======<");
        conn.close();
    }

    public void createRunnerModel(String source, String target) {

        RunnerModel model = 
          createRunner(RunnerModel.REPLICA_RUNNER_CLASS, source, target, "description");
        StrategyModel strategyModel = createStrategy(
                "ru.taximaxim.dbreplicator2.replica.strategies.DummyStrategy",
                null, true, 100);

        model.getStrategyModels().add(strategyModel);
        strategyModel.setRunner(model);

        session.beginTransaction();
        session.saveOrUpdate(model);
        session.getTransaction().commit();
        
    }
    
    public void CreateTeble(BoneCPSettingsService cpSettingsService, String poolName)
            throws SQLException, ClassNotFoundException {

        Class.forName("org.h2.Driver");
        conn = getConnection(cpSettingsService, poolName); 

        stat = conn.createStatement();

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
    }

    public Connection getConnection(BoneCPSettingsService cpSettingsService,
            String poolName) throws ClassNotFoundException, SQLException {
        BoneCPConnectionsFactory connectionsFactory = new BoneCPConnectionsFactory(
                cpSettingsService);
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

    public void delete(Statement stat, String tableName) throws SQLException {

        stat.execute("Drop Table if exists " + tableName);
    }

    public void createTrigger(Statement stat, String tableName) throws SQLException {

        String Trigger = H2CreateSetting.MyTrigger.class.getName();
        stat.execute("CREATE TRIGGER " + tableName + "_INS AFTER INSERT ON " + tableName
                + " FOR EACH ROW CALL \"" + Trigger + "\"");
        stat.execute("CREATE TRIGGER " + tableName + "_UPD AFTER UPDATE ON " + tableName
                + " FOR EACH ROW CALL \"" + Trigger + "\"");
        stat.execute("CREATE TRIGGER " + tableName + "_DEL AFTER DELETE ON " + tableName
                + " FOR EACH ROW CALL \"" + Trigger + "\"");
    }

    public static class MyTrigger implements Trigger {

        String tableName = null;
        int type = -1;

        public void init(Connection conn, String schemaName, String triggerName,
                String tableName, boolean before, int type) {
            // Initializing trigger
            this.tableName = tableName;
            this.type = type;
        }

        public void fire(Connection conn, Object[] oldRow, Object[] newRow)
                throws SQLException {

            String operation = null;
            Object id_foreign = null;

            if (type == 2) {
                operation = "U";
                id_foreign = newRow[0];
            } else if (type == 1) {
                operation = "I";
                id_foreign = newRow[0];
            } else if (type == 4) {
                operation = "D";
                id_foreign = oldRow[0];
            }

            PreparedStatement prep = conn.prepareStatement("INSERT INTO rep2_superlog "
                    + "(id_foreign, id_table, c_operation, c_date, id_transaction)"
                    + " VALUES(?, ?, ?, now(), ?)");
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

    // /////////////////////////////////////////////////////////////////////////////////////////////

    public RunnerModel createRunner(String className, String source, String target,
            String description) {

        RunnerModel runner = new RunnerModel();
        runner.setClassName(className);
        runner.setSource(source);
        runner.setTarget(target);
        runner.setDescription(description);

        return runner;
    }

    public StrategyModel createStrategy(String className, String param,
            boolean isEnabled, int priority) {

        StrategyModel strategy = new StrategyModel();

        strategy.setClassName(className);
        strategy.setParam(param);
        strategy.setEnabled(isEnabled);
        strategy.setPriority(priority);

        return strategy;
    }
}
