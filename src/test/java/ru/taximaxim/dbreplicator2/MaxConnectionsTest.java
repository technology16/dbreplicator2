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
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractHikariCPTest;
import ru.taximaxim.dbreplicator2.cf.HikariCPConnectionsFactory;
import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.model.HikariCPSettingsModel;

/**
 * Класс для тестирования пулов соединений
 *
 * @author volodin_aa
 *
 */
public class MaxConnectionsTest extends AbstractHikariCPTest {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp();    
        initialization();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        close();
    }

    /**
     * Инициализация
     */
    public static void initialization() {
        settingStorage.setDataBaseSettings(new HikariCPSettingsModel("1",
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test", "sa", ""));

        settingStorage.setDataBaseSettings(new HikariCPSettingsModel("2",
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test", "sa", ""));

        settingStorage.setDataBaseSettings(new HikariCPSettingsModel("3",
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test", "sa", ""));

        settingStorage.setDataBaseSettings(new HikariCPSettingsModel("4",
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test", "sa", "",
                5, false, 10000, 10000, 10000));
    }
    
    /**
     * Тест таймаута при привышении максимального количества открытых соединений
     *
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
    public void testMaxConnections() throws ClassNotFoundException,
            SQLException {
        ConnectionFactory connectionsFactory = new HikariCPConnectionsFactory(
                settingStorage);
        try {
            Connection connection1 = connectionsFactory.get("1").getConnection();
            connection1.setAutoCommit(false);
            connection1.commit();

            Connection connection2 = connectionsFactory.get("2").getConnection();
            connection2.setAutoCommit(false);
            connection2.commit();

            Connection connection3 = connectionsFactory.get("3").getConnection();
            connection3.setAutoCommit(false);
            connection3.commit();

            Connection connection4 = connectionsFactory.get("4").getConnection();
            connection4.setAutoCommit(false);
            connection4.commit();

            Connection connection42 = connectionsFactory.get("4").getConnection();
            connection42.setAutoCommit(false);
            connection42.commit();

            Connection connection43 = connectionsFactory.get("4").getConnection();
            connection43.setAutoCommit(false);
            connection43.commit();

            Connection connection44 = connectionsFactory.get("4").getConnection();
            connection44.setAutoCommit(false);
            connection44.commit();

            connection1.close();
            connection2.close();
            connection3.close();
            connection4.close();
            connection42.close();
            connection43.close();
            connection44.close();
        } catch (SQLException e) {
            // Поглощаем только ошибку таймаута
            if (!e.getSQLState().equals("08001")) {
                throw e;
            }
        } finally {
            connectionsFactory.close();
        }

    }

}
