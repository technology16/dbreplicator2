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
import org.junit.Assert;

/**
 * @author mardanov_rm
 */

public class Helper {
    
    protected static final Logger LOG = Logger.getLogger(Helper.class);
    
    /**
     * Сравнивание записи по листам
     * @param listSource
     * @param listDest
     */
    public static void AssertEquals(List<MyTablesType> listSource, List<MyTablesType> listDest){
        if(listSource.size() != listDest.size()) {
            LOG.error(String.format("Количество записей не равны [%s == %s]", listSource.size(), listDest.size()));
        }
        Assert.assertEquals(listSource.size(), listDest.size());
        
        if(!listSource.equals(listDest)) {
            LOG.info("====================================================================");
            for (int i = 0; i < listSource.size(); i++) {
                
                if(listSource.get(i)._int != listDest.get(i)._int) {
                    LOG.error(String.format("_int [%s == %s]", listSource.get(i)._int, listDest.get(i)._int));
                }
                Assert.assertEquals(listSource.get(i)._int, listDest.get(i)._int);
                
                if(listSource.get(i)._boolean != listDest.get(i)._boolean) {
                    LOG.error(String.format("_boolean [%s == %s]", listSource.get(i)._boolean, listDest.get(i)._boolean));
                }
                Assert.assertEquals(listSource.get(i)._boolean, listDest.get(i)._boolean);
                
                if(!listSource.get(i)._long.equals(listDest.get(i)._long)){
                    LOG.error(String.format("_long [%s == %s]", listSource.get(i)._long, listDest.get(i)._long));
                }
                Assert.assertEquals(listSource.get(i)._long, listDest.get(i)._long);
                
                if(listSource.get(i)._decimal != listDest.get(i)._decimal) {
                    LOG.error(String.format("_decimal [%s == %s]", listSource.get(i)._decimal, listDest.get(i)._decimal));
                }
                Assert.assertEquals(listSource.get(i)._decimal, listDest.get(i)._decimal);
                
                if(listSource.get(i)._string != listDest.get(i)._string){
                    LOG.error(String.format("_string [%s == %s]", listSource.get(i)._string, listDest.get(i)._string));
                }
                Assert.assertEquals(listSource.get(i)._string, listDest.get(i)._string);
                
                if(listSource.get(i)._byte != listDest.get(i)._byte){
                    LOG.error(String.format("_byte [%s == %s]", listSource.get(i)._byte, listDest.get(i)._byte));
                }
                Assert.assertEquals(listSource.get(i)._byte, listDest.get(i)._byte);
                
                
                if(((listSource.get(i)._date != null) && (listDest.get(i)._date != null))&&
                (!listSource.get(i)._date.equals(listDest.get(i)._date))) {
                        LOG.error(String.format("_date [%s == %s]", listSource.get(i)._date, listDest.get(i)._date));
                }
                Assert.assertEquals(listSource.get(i)._date, listDest.get(i)._date);
                
                if(((listSource.get(i)._time != null) && (listDest.get(i)._time != null))&&
                (!listSource.get(i)._time.equals(listDest.get(i)._time))){
                    LOG.error(String.format("_time [%s == %s]", listSource.get(i)._time, listDest.get(i)._time));
                }
                Assert.assertEquals(listSource.get(i)._time, listDest.get(i)._time);
                
                if(((listSource.get(i)._timestamp != null) && (listDest.get(i)._timestamp != null))&&
                (!listSource.get(i)._timestamp.equals(listDest.get(i)._timestamp))){
                    LOG.error(String.format("_timestamp [%s == %s]", listSource.get(i)._timestamp, listDest.get(i)._timestamp));
                }
                Assert.assertEquals(listSource.get(i)._timestamp, listDest.get(i)._timestamp);
                
                if(listSource.get(i)._double != listDest.get(i)._double) {
                    LOG.error(String.format("_double [%s == %s]", listSource.get(i)._double, listDest.get(i)._double));
                    Assert.assertEquals(1, 0);
                }
               
                if(listSource.get(i)._float != listDest.get(i)._float) {
                    LOG.error(String.format("_float [%s == %s]", listSource.get(i)._float, listDest.get(i)._float));
                    Assert.assertEquals(1, 0);
                }
            }
            LOG.info("====================================================================");
        }
    }
    
    /**
     * Вывод информации из листа
     * @param list
     */
    public static void InfoList(List<MyTablesType> list) {
            for (int i = 0; i < list.size(); i++) {
                LOG.info("====================================================================");
                LOG.info(String.format("id [%s]", list.get(i).id));
                LOG.info(String.format("_int [%s]", list.get(i)._int));
                LOG.info(String.format("_boolean [%s]", list.get(i)._boolean));
                LOG.info(String.format("_long [%s]", list.get(i)._long));
                LOG.info(String.format("_decimal [%s]", list.get(i)._decimal));
                LOG.info(String.format("_string [%s]", list.get(i)._string));
                LOG.info(String.format("_byte [%s]", list.get(i)._byte));
                LOG.info(String.format("_date [%s]", list.get(i)._date));
                LOG.info(String.format("_time [%s]", list.get(i)._time));
                LOG.info(String.format("_timestamp [%s]", list.get(i)._timestamp));
                LOG.info(String.format("_double [%s]", list.get(i)._double));
                LOG.info(String.format("_float [%s]", list.get(i)._float));
                LOG.info("====================================================================");
                
            }
        }
    
    /**
     * Запись в лист выполняя запрос select * from tableName order by id
     * @param conn - соединение
     * @param tableName - имя талицы
     * @return List<MyTablesType>
     * @throws SQLException
     */
    public static List<MyTablesType> InfoTest(Connection conn, String tableName) throws SQLException{
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from " + tableName + " order by id");
        List<MyTablesType> list = new ArrayList<MyTablesType>();
        MyTablesType tab = null;
        while (rs.next()) {
            
            tab = new MyTablesType();
            tab.id = rs.getInt("id");
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
        rs.close();
        stat.close();
        return list;
    }
    
    /**
     * Проверка null select * from tableName where _int = _int
     * @param conn - соединение 
     * @param tableName - имя таблицы
     * @param _int - поисково значение
     * @throws SQLException
     */
    public static void InfoNull(Connection conn, String tableName, Integer _int) throws SQLException{
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select * from " + tableName + " where _int = " + _int);
        while (rs.next()) {
            if(rs.getObject("_long")!=null) {
                LOG.error(String.format("_long not null [%s]", rs.getObject("_long").toString()));
            }
            Assert.assertNull(rs.getObject("_long"));
            
            if(rs.getObject("_decimal")!=null) {
                LOG.error(String.format("_decimal not null [%s]", rs.getObject("_decimal").toString()));
            }
            Assert.assertNull(rs.getObject("_decimal"));
            
            if(rs.getObject("_double")!=null) {
                LOG.error(String.format("_double not null [%s]", rs.getObject("_double").toString()));
            }
            Assert.assertNull(rs.getObject("_double"));
            
            if(rs.getObject("_float")!=null) {
                LOG.error(String.format("_float not null [%s]", rs.getObject("_float").toString()));
            }
            Assert.assertNull(rs.getObject("_float"));
            
            if(rs.getObject("_string")!=null) {
                LOG.error(String.format("_string not null [%s]", rs.getObject("_string").toString()));
            }
            Assert.assertNull(rs.getObject("_string"));
            
            if(rs.getObject("_byte")!=null) {
                LOG.error(String.format("_byte not null [%s]", rs.getObject("_byte").toString()));
            }
            Assert.assertNull(rs.getObject("_byte"));
            
            if(rs.getObject("_date")!=null) {
                LOG.error(String.format("_date not null [%s]", rs.getObject("_date").toString()));
            }
            Assert.assertNull(rs.getObject("_date"));
            
            if(rs.getObject("_time")!=null) {
                LOG.error(String.format("_time not null [%s]", rs.getObject("_time").toString()));
            }
            Assert.assertNull(rs.getObject("_time"));
            
            if(rs.getObject("_timestamp")!=null) {
                LOG.error(String.format("_timestamp not null [%s]", rs.getObject("_timestamp").toString()));
            }
            Assert.assertNull(rs.getObject("_timestamp"));
        }
        rs.close();
        stat.close();
    }
    
    /**
     * Выполнение запроса выборки
     * select * from tableName
     * 
     * @param conn - Соединение
     * @param tableName - Имя таблицы
     * @throws SQLException
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
        
        String[] triggers = sqlText.split("<#CreateTrigger#>");
        if (triggers.length==3) {
            String[] tableNames = triggers[1].split(",");
            createTriggers(connection, tableNames);
        }
    }
    
    protected static void executeSqlFromFiles(Connection connection, String[] fileNames) throws IOException, SQLException{
        for (String fileName : fileNames) {
            executeSqlFromFile(connection, fileName);
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

    /**
     * Создание тригерров
     * @param conn - Соединение
     * @param tableNames - список таблиц
     * @throws SQLException
     */
    public static void createTriggers(Connection conn, String[] tableNames) throws SQLException{
        for (String tableName : tableNames) {
            createTrigger(conn, tableName);
        }
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