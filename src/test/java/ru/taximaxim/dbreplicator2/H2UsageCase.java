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
import java.sql.DriverManager;
import java.sql.SQLException;

import org.h2.tools.Server;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Тест соединения с базой данных по протоколу TCP.
 * 
 * @author ags
 */
public class H2UsageCase {

    private static final String TCP_PORT = "8084";
    private static final String JDBC_URL = "jdbc:h2:tcp://localhost:"
            + TCP_PORT + "/~/H2UsageCase";
    private static final String JDBC_USER = "sa";
    private static final String JDBC_PASSWORD = "";

    private static Server server;

    private static Connection conn; 
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        Class.forName("org.h2.Driver");
        server = Server.createTcpServer(
                new String[] { "-tcpPort", TCP_PORT, "-tcpAllowOthers" })
                .start();
    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {
        server.stop();
        if(conn!=null)
            conn.close();
    }

    protected Connection getConnection() throws SQLException {
        return DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PASSWORD);
    }

    @Test
    public void testConnection() throws SQLException {
        conn = getConnection();
    }
}
