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

import ru.taximaxim.dbreplicator2.model.BoneCPSettingsModel;
import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.model.StrategyModel;

public class Helper {
    
    protected static final Logger LOG = Logger.getLogger(Helper.class);
    
    public static void setupConn(Session session, String name)
            throws ClassNotFoundException, SQLException {
        BoneCPSettingsModel settings = new BoneCPSettingsModel(name,"org.h2.Driver", "jdbc:h2:mem://localhost/~/" + name, "sa", "");
        session.beginTransaction();
        session.saveOrUpdate(settings);
        session.getTransaction().commit();
    }

    public static void CreateTebleRep2(Connection conn) throws SQLException, ClassNotFoundException {

        try {
            Statement stat = conn.createStatement();

            delete(stat, "rep2_superlog");
            delete(stat, "rep2_workpool_data");

            stat.execute("CREATE TABLE rep2_superlog(id_superlog IDENTITY PRIMARY KEY, id_foreign INTEGER, id_table NVARCHAR, c_operation VARCHAR(1), c_date TIMESTAMP, id_transaction NVARCHAR);");
            stat.execute("CREATE TABLE rep2_workpool_data(id_runner INTEGER, id_superlog BIGINT, id_foreign INTEGER, id_table NVARCHAR, c_operation VARCHAR(1), c_date TIMESTAMP, id_transaction NVARCHAR);");

        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public static void delete(Statement stat, String tableName) throws SQLException {
        try {
            stat.execute("Drop Table if exists " + tableName);
        } catch (SQLException e) {
            e.printStackTrace();
            throw e;
        }
    }
    
    public static void InfoTest(Connection conn) throws SQLException{
        Statement stat = conn.createStatement();
        ResultSet rsS = stat.executeQuery("select * from runners");
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
    }
    
    public static void InfoSuperLog(Connection conn) throws SQLException{
        Statement stat = conn.createStatement();
        ResultSet rsS = stat.executeQuery("select id_superlog, id_foreign, id_table, c_operation, c_date, id_transaction from rep2_superlog");
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
    }
    
    public static void InfoWorkPoolData(Connection conn) throws SQLException{
        
        Statement stat = conn.createStatement();
        ResultSet rsQ = stat.executeQuery("select id_runner, id_superlog, id_foreign, id_table, c_operation, c_date, id_transaction from rep2_workpool_data");
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
    }

    public static void createTrigger(Connection conn, String tableName) throws SQLException {

        Statement stat = conn.createStatement();
        String Trigger = Helper.MyTrigger.class.getName();
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

    /**
     * Создание RunnerModel
     * @return RunnerModel
     */
    public static RunnerModel createRunner(String className, String source, String target,
            String description) {

        RunnerModel runner = new RunnerModel();
        runner.setClassName(className);
        runner.setSource(source);
        runner.setTarget(target);
        runner.setDescription(description);

        return runner;
    }

    /**
     * Создание старатегии
     * @return StrategyModel
     */
    public static StrategyModel createStrategy(String className, String param,
            boolean isEnabled, int priority) {

        StrategyModel strategy = new StrategyModel();

        strategy.setClassName(className);
        strategy.setParam(param);
        strategy.setEnabled(isEnabled);
        strategy.setPriority(priority);

        return strategy;
    }
    

    /**
     * Создание RunnerModel и Запись в базу 
     */
    public static void createRunnerModel(Session session, String source, String target) {

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
}
