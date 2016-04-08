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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

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

    // Задержка между циклами репликации
    private static final int REPLICATION_DELAY = 100;
    protected static final Logger LOG = Logger.getLogger(Helper.class);
    
    /**
     * Сравнивание записи по листам
     * @param listSource
     * @param listDest
     * @throws InterruptedException 
     */
    public static void AssertEquals(List<MyTablesType> listSource, List<MyTablesType> listDest) throws InterruptedException{
        Thread.sleep(REPLICATION_DELAY);
        assertTrue(String.format("Количество записей не равны [%s == %s]", listSource.size(), listDest.size()),
                listSource.size() == listDest.size());
        
        if(!listSource.equals(listDest)) {
            LOG.info("====================================================================");
            for (int i = 0; i < listSource.size(); i++) {
                
                assertTrue(String.format("Ошибка в поле _int [%s != %s]", listSource.get(i)._int, listDest.get(i)._int), 
                        listSource.get(i)._int == listDest.get(i)._int);
                
                assertTrue(String.format("Ошибка в поле _boolean [%s != %s]", listSource.get(i)._boolean, listDest.get(i)._boolean), 
                        listSource.get(i)._boolean == listDest.get(i)._boolean);
                
                assertTrue(String.format("Ошибка в поле _long [%s == %s]", listSource.get(i)._long, listDest.get(i)._long), 
                        listSource.get(i)._long.equals(listDest.get(i)._long));
                
                assertEquals("Ошибка в поле _decimal", listSource.get(i)._decimal, listDest.get(i)._decimal);
                
                //==============================
                // игнорируемая колонка
                assertTrue(String.format("Ошибка в поле _string [%s != %s]", listSource.get(i)._string, listDest.get(i)._string), 
                        listSource.get(i)._string != listDest.get(i)._string);
                //==============================
                
                assertTrue(String.format("Ошибка в поле _byte [%s == %s]", listSource.get(i)._byte, listDest.get(i)._byte), 
                        listSource.get(i)._byte == listDest.get(i)._byte);
                
                Assert.assertNotNull(String.format("Ошибка нулевое значение в поле _date [%s]", listSource.get(i)._date), listSource.get(i)._date);
                Assert.assertNotNull(String.format("Ошибка нулевое значение в поле _date [%s]", listDest.get(i)._date), listDest.get(i)._date);
                assertTrue(String.format("Ошибка в поле _date [%s == %s]", listSource.get(i)._date, listDest.get(i)._date), 
                        listSource.get(i)._date.equals(listDest.get(i)._date));
                
                Assert.assertNotNull(String.format("Ошибка нулевое значение в поле _time [%s]", listSource.get(i)._time), listSource.get(i)._time);
                Assert.assertNotNull(String.format("Ошибка нулевое значение в поле _time [%s]", listDest.get(i)._time), listDest.get(i)._time);
                assertTrue(String.format("Ошибка в поле _time [%s == %s]", listSource.get(i)._time, listDest.get(i)._time), 
                        listSource.get(i)._time.equals(listDest.get(i)._time));
                
                Assert.assertNotNull(String.format("Ошибка нулевое значение в поле _timestamp [%s]", listSource.get(i)._timestamp), listSource.get(i)._timestamp);
                Assert.assertNotNull(String.format("Ошибка нулевое значение в поле _timestamp [%s]", listDest.get(i)._timestamp), listDest.get(i)._timestamp);
                assertTrue(String.format("Ошибка в поле _timestamp [%s == %s]", listSource.get(i)._timestamp, listDest.get(i)._timestamp), 
                        listSource.get(i)._timestamp.equals(listDest.get(i)._timestamp));
                
                
                assertTrue(String.format("Ошибка в поле _double [%s == %s]", listSource.get(i)._double, listDest.get(i)._double), 
                        listSource.get(i)._double == listDest.get(i)._double);
               
                
                assertTrue(String.format("Ошибка в поле _float [%s == %s]", listSource.get(i)._float, listDest.get(i)._float), 
                        listSource.get(i)._float == listDest.get(i)._float);
            }
            LOG.info("====================================================================");
        }
    }
    /**
     * Сравнивание записи по листам
     * @param listSource
     * @param listDest
     * @throws InterruptedException 
     */
    public static void AssertEqualsIgnoreReplication(List<MyTablesType> listSource, List<MyTablesType> listDest) throws InterruptedException{
        Thread.sleep(REPLICATION_DELAY);
        assertTrue(String.format("Количество записей не равны [%s == %s]", listSource.size(), listDest.size()),
                listSource.size() == listDest.size());
        
        if(!listSource.equals(listDest)) {
            LOG.info("====================================================================");
            for (int i = 0; i < listSource.size(); i++) {
                
                //==============================
                // реплицуруемая колонка
                assertTrue(String.format("Ошибка в поле _int [%s != %s]", listSource.get(i)._int, listDest.get(i)._int), 
                        listSource.get(i)._int == listDest.get(i)._int);
                
                assertTrue(String.format("Ошибка в поле _boolean [%s != %s]", listSource.get(i)._boolean, listDest.get(i)._boolean), 
                        listSource.get(i)._boolean == listDest.get(i)._boolean);
              //==============================
                
                assertTrue(String.format("Ошибка Реплицировалась колонка не включенная в репликацию _long [%s != %s]", listSource.get(i)._long, listDest.get(i)._long), 
                        !listSource.get(i)._long.equals(listDest.get(i)._long));
                
                assertTrue(String.format("Ошибка Реплицировалась колонка не включенная в репликацию _decimal [%s != %s]", listSource.get(i)._decimal, listDest.get(i)._decimal), 
                        listSource.get(i)._decimal != listDest.get(i)._decimal);
                
                //==============================
                // игнорируемая колонка
                assertTrue(String.format("Ошибка в поле _string [%s != %s]", listSource.get(i)._string, listDest.get(i)._string), 
                        listSource.get(i)._string != listDest.get(i)._string);
                //==============================
                
                assertTrue(String.format("Ошибка Реплицировалась колонка не включенная в репликацию _byte [%s != %s]", listSource.get(i)._byte, listDest.get(i)._byte), 
                        listSource.get(i)._byte != listDest.get(i)._byte);
                
                //==============================
                // реплицуруемая колонка
                Assert.assertNotNull(String.format("Ошибка нулевое значение в поле _date [%s]", listSource.get(i)._date), listSource.get(i)._date);
                Assert.assertNotNull(String.format("Ошибка нулевое значение в поле _date [%s]", listDest.get(i)._date), listDest.get(i)._date);
                assertTrue(String.format("Ошибка в поле _date [%s == %s]", listSource.get(i)._date, listDest.get(i)._date), 
                        listSource.get(i)._date.equals(listDest.get(i)._date));
                
                Assert.assertNotNull(String.format("Ошибка нулевое значение в поле _time [%s]", listSource.get(i)._time), listSource.get(i)._time);
                Assert.assertNotNull(String.format("Ошибка нулевое значение в поле _time [%s]", listDest.get(i)._time), listDest.get(i)._time);
                assertTrue(String.format("Ошибка в поле _time [%s == %s]", listSource.get(i)._time, listDest.get(i)._time), 
                        listSource.get(i)._time.equals(listDest.get(i)._time));
                
                Assert.assertNotNull(String.format("Ошибка нулевое значение в поле _timestamp [%s]", listSource.get(i)._timestamp), listSource.get(i)._timestamp);
                Assert.assertNotNull(String.format("Ошибка нулевое значение в поле _timestamp [%s]", listDest.get(i)._timestamp), listDest.get(i)._timestamp);
                assertTrue(String.format("Ошибка в поле _timestamp [%s == %s]", listSource.get(i)._timestamp, listDest.get(i)._timestamp), 
                        listSource.get(i)._timestamp.equals(listDest.get(i)._timestamp));
              //==============================
                
                assertTrue(String.format("Ошибка Реплицировалась колонка не включенная в репликацию  _double [%s != %s]", listSource.get(i)._double, listDest.get(i)._double), 
                        listSource.get(i)._double != listDest.get(i)._double);
               
                
                assertTrue(String.format("Ошибка Реплицировалась колонка не включенная в репликацию  _float [%s != %s]", listSource.get(i)._float, listDest.get(i)._float), 
                        listSource.get(i)._float != listDest.get(i)._float);
            }
            LOG.info("====================================================================");
        }
    }
    
    /**
     * Сравнивание записи по листам
     * @param listSource
     * @param listDest
     * @throws InterruptedException 
     */
    public static void AssertEqualsNull(List<MyTablesType> listSource, List<MyTablesType> listDest) throws InterruptedException{
        Thread.sleep(REPLICATION_DELAY);
        if(listSource.size() != listDest.size()) {
            LOG.error(String.format("Количество записей не равны [%s == %s]", listSource.size(), listDest.size()));
        }
        Assert.assertEquals(listSource.size(), listDest.size());
        
        if(!listSource.equals(listDest)) {
            LOG.info("====================================================================");
            for (int i = 0; i < listSource.size(); i++) {
                
                assertTrue(String.format("Ошибка в поле _int [%s != %s]", listSource.get(i)._int, listDest.get(i)._int), 
                        listSource.get(i)._int == listDest.get(i)._int);
                
                assertTrue(String.format("Ошибка в поле _boolean [%s != %s]", listSource.get(i)._boolean, listDest.get(i)._boolean), 
                        listSource.get(i)._boolean == listDest.get(i)._boolean);
                
                assertTrue(String.format("Ошибка в поле _long [%s == %s]", listSource.get(i)._long, listDest.get(i)._long), 
                        listSource.get(i)._long.equals(listDest.get(i)._long));
                
                assertTrue(String.format("Ошибка в поле _decimal [%s == %s]", listSource.get(i)._decimal, listDest.get(i)._decimal), 
                        listSource.get(i)._decimal == listDest.get(i)._decimal);
                
                //==============================
                // игнорируемая колонка
                assertTrue(String.format("Ошибка в поле _string [%s == %s]", listSource.get(i)._string, listDest.get(i)._string), 
                        listSource.get(i)._string == listDest.get(i)._string);
                //==============================
                
                assertTrue(String.format("Ошибка в поле _byte [%s == %s]", listSource.get(i)._byte, listDest.get(i)._byte), 
                        listSource.get(i)._byte == listDest.get(i)._byte);
                
                Assert.assertNull(String.format("Ошибка нулевое значение в поле _date [%s]", listSource.get(i)._date), listSource.get(i)._date);
                Assert.assertNull(String.format("Ошибка нулевое значение в поле _date [%s]", listDest.get(i)._date), listDest.get(i)._date);
                assertTrue(String.format("Ошибка в поле _date [%s == %s]", listSource.get(i)._date, listDest.get(i)._date), 
                        listSource.get(i)._date==listDest.get(i)._date);
                
                Assert.assertNull(String.format("Ошибка нулевое значение в поле _time [%s]", listSource.get(i)._time), listSource.get(i)._time);
                Assert.assertNull(String.format("Ошибка нулевое значение в поле _time [%s]", listDest.get(i)._time), listDest.get(i)._time);
                assertTrue(String.format("Ошибка в поле _time [%s == %s]", listSource.get(i)._time, listDest.get(i)._time), 
                        listSource.get(i)._time==listDest.get(i)._time);
                
                Assert.assertNull(String.format("Ошибка нулевое значение в поле _timestamp [%s]", listSource.get(i)._timestamp), listSource.get(i)._timestamp);
                Assert.assertNull(String.format("Ошибка нулевое значение в поле _timestamp [%s]", listDest.get(i)._timestamp), listDest.get(i)._timestamp);
                assertTrue(String.format("Ошибка в поле _timestamp [%s == %s]", listSource.get(i)._timestamp, listDest.get(i)._timestamp), 
                        listSource.get(i)._timestamp==listDest.get(i)._timestamp);
                
                
                assertTrue(String.format("Ошибка в поле _double [%s == %s]", listSource.get(i)._double, listDest.get(i)._double), 
                        listSource.get(i)._double == listDest.get(i)._double);
               
                
                assertTrue(String.format("Ошибка в поле _float [%s == %s]", listSource.get(i)._float, listDest.get(i)._float), 
                        listSource.get(i)._float == listDest.get(i)._float);
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
     * @throws InterruptedException 
     */
    public static List<MyTablesType> InfoTest(Connection conn, String tableName) throws SQLException, InterruptedException{
        Thread.sleep(REPLICATION_DELAY);
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
     * @throws InterruptedException 
     */
    public static void InfoNull(Connection conn, String tableName, Integer _int) throws SQLException, InterruptedException{
        Thread.sleep(REPLICATION_DELAY);
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
     * @throws InterruptedException 
     */
    public static void InfoSelect(Connection conn,  String tableName) throws SQLException, InterruptedException{
        Thread.sleep(REPLICATION_DELAY);
        LOG.info("select * from " + tableName + "[count = " + InfoCount(conn,  tableName)+"]");
        Statement statSource = conn.createStatement();
        ResultSet rsSource = statSource.executeQuery("select * from " + tableName);
        ResultSetMetaData rDataSource = rsSource.getMetaData();
        int totalColumnSource = rDataSource.getColumnCount();
        while (rsSource.next()) {
            String text = "|";
            for (int i = 1; i <= totalColumnSource; i++) {
                text += rsSource.getObject(i) + "|";
            }
            LOG.info(text);
           // LOG.info("================================================================");
        }
        rsSource.close();
        statSource.close();
    }
    
    public static int InfoCount(Connection conn,  String tableName) throws SQLException{
        Statement stat = conn.createStatement();
        ResultSet rs = stat.executeQuery("select count(*) as count from " + tableName);
        int count = 0;
        while (rs.next()) {
            count = rs.getInt(1);
        }
        rs.close();
        stat.close();
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
    public static void executeSqlFromFile(Connection connection, String fileName) throws IOException, SQLException{
        executeSqlFromFile(connection, fileName, 0);
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
    protected static void executeSqlFromFile(Connection connection, String fileName, int id) throws IOException, SQLException{
        String curDir = new File("").getAbsolutePath() + "/src/test/resources/" + fileName;
        FileInputStream sqlFile = new FileInputStream(curDir);
        byte[] sqlBytes = new byte[sqlFile.available()];
        sqlFile.read(sqlBytes);
        sqlFile.close();

        String sqlText = new String(sqlBytes);
        
        PreparedStatement statement = connection.prepareStatement(sqlText.replace("?", ""+id));
        statement.execute();
        
        String[] triggers = sqlText.split("<#CreateTrigger#>");
        if (triggers.length==3) {
            String[] tableNames = triggers[1].split(",");
            createTriggers(connection, tableNames);
        }
        
        connection.commit();
        statement.close();
    }
    
    protected static void executeSqlFromSql(Connection connection, String sql, String name) throws IOException, SQLException{
        PreparedStatement statement = connection.prepareStatement(sql);
        statement.setString(1, name);
        statement.execute();
        connection.commit();
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
            String id_pool = "";
            PreparedStatement prep2 = conn.prepareStatement("SELECT _value FROM T_TAB");
            ResultSet sourceResultSet = prep2.executeQuery();
            while (sourceResultSet.next()) {
                id_pool =  sourceResultSet.getString("_value");
            }
            
            PreparedStatement prep = conn.prepareStatement("INSERT INTO rep2_superlog "
                    + "(id_foreign, id_table, c_operation, c_date, id_transaction, id_pool)"
                    + " VALUES(?, ?, ?, now(), ?, ?)");
            prep.setObject(1, id_foreign);
            prep.setObject(2, tableName);
            prep.setObject(3, operation);
            prep.setObject(4, 0);
            prep.setObject(5, id_pool);
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
