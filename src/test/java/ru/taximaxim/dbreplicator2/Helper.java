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
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.h2.api.Trigger;

/**
 * @author mardanov_rm
 */

public class Helper {
    
    protected static final Logger LOG = Logger.getLogger(Helper.class);
    
    public static List<MyTablesType> InfoTest(Connection conn, String tableName) throws SQLException{
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from " + tableName);
        List<MyTablesType> list = new ArrayList<MyTablesType>();
        MyTablesType tab = null;
        while (rs.next()) {
            
            tab = new MyTablesType();
            tab._int = rs.getInt("_int");
            tab._boolean = rs.getBoolean("_boolean");
            tab._long = rs.getLong("_long");
            tab._decimal = rs.getBigDecimal("_decimal"); 
            tab._double = rs.getDouble("_double");
            tab._float = rs.getFloat("_float");
            tab._string = rs.getString("_string");
            tab._byte = rs.getByte("_byte");
            tab._date = rs.getDate("_date");
            tab._time = rs.getTime("_time");
            tab._timestamp = rs.getTimestamp("_timestamp");
            list.add(tab);
        }
        return list;
    }
    
    /**
     * Выполнение запроса выборки
     */
    public static void InfoSelect(Connection conn,  String tableName) throws SQLException{
        Statement statSource = conn.createStatement();
        ResultSet rsSource = statSource.executeQuery("select * from " + tableName);
        ResultSetMetaData rDataSource = rsSource.getMetaData();
        int totalColumnSource = rDataSource.getColumnCount();
        while (rsSource.next()) {
            String text = "";
            for (int i = 1; i <= totalColumnSource; i++) {
                text += rsSource.getObject(i).toString() + "\t";
            }
            LOG.info(text);
            LOG.info("================================================================");
        }
    }
    
    public static int InfoCount(Connection conn,  String tableName) throws SQLException{
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select count(*) as count from " + tableName);
        int count = 0;
        while (rs.next()) {
            count = rs.getInt(1);
        }
        return count;
    }
    
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