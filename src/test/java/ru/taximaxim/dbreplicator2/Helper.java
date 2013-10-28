package ru.taximaxim.dbreplicator2;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.h2.api.Trigger;

public class Helper {
    
    protected static final Logger LOG = Logger.getLogger(Helper.class);

    /**
     * Выполнение скрипта из текстового файла
     * 
     * @param connection - соединение с целевой БД
     * @param fileName - полное имя файла
     * 
     * @throws IOException
     * @throws SQLException
     */
    protected static void executeSqlFromFile(Connection connection, String fileName) throws IOException, SQLException{
        String curDir = new File("").getAbsolutePath() + "/src/test/resources/" + fileName;
        FileInputStream sqlFile = new FileInputStream(curDir);
        byte[] sqlBytes = new byte[sqlFile.available()];
        sqlFile.read(sqlBytes);
        sqlFile.close();

        String sqlText = new String(sqlBytes);

        PreparedStatement statement = connection.prepareStatement(sqlText);
        statement.execute();
    }
    
    /**
     * Вывод таблицы rep2_superlog
     */
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
    
    /**
     * Вывод таблицы rep2_workpool_data
     */
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
            LOG.info("========================================================================");
        }
    }

    /**
     * Создание Триггера
     */
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
}
