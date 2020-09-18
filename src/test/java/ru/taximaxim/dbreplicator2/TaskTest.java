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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import java.sql.SQLException;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractHikariCPTest;
import ru.taximaxim.dbreplicator2.model.HikariCPSettingsModel;
/**
 * Класс для тестирования пулов соединений
 *
 * @author volodin_aa
 *
 */
public class TaskTest extends AbstractHikariCPTest  {

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        close();
    }

    /**
     * Проверка получения настроек по имени:
     *  1. Полуение несуществующих настроек
     *  2. Получение существующих
     *
     * @throws SQLException
     */
    @Test
    public void testGetTask() throws SQLException {
        // Полуение несуществующих настроек
        HikariCPSettingsModel hikariCPSettings = settingStorage.getDataBaseSettingsByName("testGetDataBaseSettingsByName");
        assertNull("Ошибка при получение несуществующих настроек!", hikariCPSettings);

        // Получение существующих
        HikariCPSettingsModel newHikariCPSettings = new HikariCPSettingsModel("testGetDataBaseSettingsByName",
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test",
                "sa",
                "");

        settingStorage.setDataBaseSettings(newHikariCPSettings);
        hikariCPSettings = settingStorage.getDataBaseSettingsByName("testGetDataBaseSettingsByName");
        assertEquals("Ошибка при получение существующих настроек по умолчанию!", newHikariCPSettings, hikariCPSettings);

        // Получение существующих
        newHikariCPSettings = new HikariCPSettingsModel("testGetDataBaseSettingsByName2",
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test",
                "sa", "", 5, false, 10000, 10000, 10000);

        settingStorage.setDataBaseSettings(newHikariCPSettings);
        hikariCPSettings = settingStorage.getDataBaseSettingsByName("testGetDataBaseSettingsByName2");
        assertEquals("Ошибка при получение существующих настроек!", newHikariCPSettings, hikariCPSettings);

        hikariCPSettings = settingStorage.getDataBaseSettingsByName("testGetDataBaseSettingsByName");
        assertNotEquals("Ошибка при получение существующих настроек!", newHikariCPSettings, hikariCPSettings);
    }

    /**
     * Тест получение всех настроек
     *
     * @throws SQLException
     */
    @Test
    public void testGetTasks() throws SQLException {
        // Создание настроек
        HikariCPSettingsModel newHikariCPSettings1 = new HikariCPSettingsModel("testGetDataBaseSettings1",
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test",
                "sa",
                "");
        settingStorage.setDataBaseSettings(newHikariCPSettings1);

        HikariCPSettingsModel newHikariCPSettings2 = new HikariCPSettingsModel("testGetDataBaseSettings2",
                "org.postgresql.Driver",
                "jdbc:postgresql://127.0.0.1:5432/LoadPullPgMsPub",
                "ags", "", 5, false, 10000, 10000, 10000);

        settingStorage.setDataBaseSettings(newHikariCPSettings2);

        Map<String, HikariCPSettingsModel> settingsMap = settingStorage.getDataBaseSettings();
        assertEquals("Ошибка при получение существующих настроек по умолчанию!",
                newHikariCPSettings1, settingsMap.get("testGetDataBaseSettings1"));
        assertEquals("Ошибка при получение существующих настроек!",
                newHikariCPSettings2, settingsMap.get("testGetDataBaseSettings2"));

    }

    /**
     * Тест таймаута при привышении максимального количества открытых соединений
     *
     * @throws SQLException
     */
    @Test
    public void testSetTask() throws SQLException {
        // Создание настроек
        HikariCPSettingsModel newHikariCPSettings = new HikariCPSettingsModel("testSetDataBaseSettings",
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test",
                "sa",
                "");

        settingStorage.setDataBaseSettings(newHikariCPSettings);
        HikariCPSettingsModel hikariCPSettings = settingStorage.getDataBaseSettingsByName("testSetDataBaseSettings");
        assertEquals("Ошибка при получение существующих настроек по умолчанию!", newHikariCPSettings, hikariCPSettings);

        // Обновление настроек
        hikariCPSettings.setMaximumPoolSize(5);
        hikariCPSettings.setInitializationFailFast(false);
        hikariCPSettings.setConnectionTimeout(10000);
        hikariCPSettings.setIdleTimeout(10000);
        hikariCPSettings.setMaxLifetime(10000);
        settingStorage.setDataBaseSettings(hikariCPSettings);
        HikariCPSettingsModel updatedBoneCPSettings = settingStorage.getDataBaseSettingsByName("testSetDataBaseSettings");
        assertEquals("Ошибка при получение обновленных настроек!", updatedBoneCPSettings, hikariCPSettings);

        // Обновление идентификатора
        hikariCPSettings.setPoolId("testSetDataBaseSettings2");
        settingStorage.setDataBaseSettings(hikariCPSettings);
        updatedBoneCPSettings = settingStorage.getDataBaseSettingsByName("testSetDataBaseSettings2");
        assertEquals("Ошибка при получение настроек с новым идентификатором!", updatedBoneCPSettings, hikariCPSettings);

        //updatedBoneCPSettings = settingStorage.getDataBaseSettingsByName("testSetDataBaseSettings");
        //assertNull("Существуют настройки со старым идентификатором!", updatedBoneCPSettings);
    }

    /**
     * Тест таймаута при привышении максимального количества открытых соединений
     *
     * @throws SQLException
     */
    @Test
    public void testDelTask() throws SQLException {
        // Создание настроек
        HikariCPSettingsModel newHikariCPSettings = new HikariCPSettingsModel("testDelDataBaseSettings",
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test",
                "sa",
                "");

        settingStorage.setDataBaseSettings(newHikariCPSettings);
        HikariCPSettingsModel hikariCPSettings = settingStorage.getDataBaseSettingsByName("testDelDataBaseSettings");
        assertEquals("Ошибка при получение существующих настроек по умолчанию!", newHikariCPSettings, hikariCPSettings);

        // Удаляем настройки
        settingStorage.delDataBaseSettings(hikariCPSettings);
        hikariCPSettings = settingStorage.getDataBaseSettingsByName("testDelDataBaseSettings");
        assertNull("Существуют удаленные настройки!", hikariCPSettings);
    }
}
