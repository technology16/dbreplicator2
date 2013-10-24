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

import static org.junit.Assert.*;

import java.sql.SQLException;
import java.util.Map;

import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistryBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.cf.BoneCPDataBaseSettingsStorage;
import ru.taximaxim.dbreplicator2.cf.BoneCPSettings;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsService;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsModel;

/**
 * Класс для тестирования пулов соединений
 * 
 * @author volodin_aa
 * 
 */
public class BoneCPSettingsServiceTest {

    static private SessionFactory sessionFactory;
    // Хранилище настроек
    static protected BoneCPDataBaseSettingsStorage settingStorage;

    @BeforeClass
    static public void setUp() throws Exception {
        Configuration configuration = new Configuration().configure();
        
        // Инициализируем Hibernate
        sessionFactory = new Configuration()
        .configure()
        .buildSessionFactory(new ServiceRegistryBuilder()
            .applySettings(configuration.getProperties()).buildServiceRegistry());

        // Инициализируем хранилище настроек пулов соединений
        settingStorage = new BoneCPSettingsService(sessionFactory);
    }

    @AfterClass
    static public void tearDown() throws Exception {
        if (sessionFactory != null) {
            sessionFactory.close();
        }
    }

    /**
     * Проверка получения настроек по имени: 1. Полуение несуществующих настроек
     * 2. Получение существующих
     * 
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
    public void testGetDataBaseSettingsByName() throws ClassNotFoundException,
            SQLException {
        // Полуение несуществующих настроек
        BoneCPSettings boneCPSettings = settingStorage
                .getDataBaseSettingsByName("testGetDataBaseSettingsByName");
        assertNull("Ошибка при получение несуществующих настроек!",
                boneCPSettings);

        // Получение существующих
        BoneCPSettings newBoneCPSettings = new BoneCPSettingsModel(
                "testGetDataBaseSettingsByName", "org.postgresql.Driver",
                "jdbc:postgresql://127.0.0.1:5432/LoadPullPgMsPub", "ags", "");

        settingStorage.setDataBaseSettings(newBoneCPSettings);
        boneCPSettings = settingStorage
                .getDataBaseSettingsByName("testGetDataBaseSettingsByName");
        assertEquals(
                "Ошибка при получение существующих настроек по умолчанию!",
                newBoneCPSettings, boneCPSettings);

        // Получение существующих
        newBoneCPSettings = new BoneCPSettingsModel(
                "testGetDataBaseSettingsByName2", "org.postgresql.Driver",
                "jdbc:postgresql://127.0.0.1:5432/LoadPullPgMsPub", "ags", "",
                1, 2, 3, 4, 5);

        settingStorage.setDataBaseSettings(newBoneCPSettings);
        boneCPSettings = settingStorage
                .getDataBaseSettingsByName("testGetDataBaseSettingsByName2");
        assertEquals("Ошибка при получение существующих настроек!",
                newBoneCPSettings, boneCPSettings);

        boneCPSettings = settingStorage
                .getDataBaseSettingsByName("testGetDataBaseSettingsByName");
        assertFalse("Ошибка при получение существующих настроек!",
                newBoneCPSettings.equals(boneCPSettings));
    }

    /**
     * Тест получение всех настроек
     * 
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
    public void testGetDataBaseSettings() throws ClassNotFoundException,
            SQLException {
        // Создание настроек
        BoneCPSettings newBoneCPSettings1 = new BoneCPSettingsModel(
                "testGetDataBaseSettings1", "org.postgresql.Driver",
                "jdbc:postgresql://127.0.0.1:5432/LoadPullPgMsPub", "ags", "");
        settingStorage.setDataBaseSettings(newBoneCPSettings1);

        BoneCPSettings newBoneCPSettings2 = new BoneCPSettingsModel(
                "testGetDataBaseSettings2", "org.postgresql.Driver",
                "jdbc:postgresql://127.0.0.1:5432/LoadPullPgMsPub", "ags", "",
                1, 2, 3, 4, 5);

        settingStorage.setDataBaseSettings(newBoneCPSettings2);

        Map<String, BoneCPSettings> settingsMap = settingStorage
                .getDataBaseSettings();
        assertEquals(
                "Ошибка при получение существующих настроек по умолчанию!",
                newBoneCPSettings1, settingsMap.get("testGetDataBaseSettings1"));
        assertEquals("Ошибка при получение существующих настроек!",
                newBoneCPSettings2, settingsMap.get("testGetDataBaseSettings2"));

    }

    /**
     * Тест таймаута при привышении максимального количества открытых соединений
     * 
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
    public void testSetDataBaseSettings() throws ClassNotFoundException,
            SQLException {
        // Создание настроек
        BoneCPSettings newBoneCPSettings = new BoneCPSettingsModel(
                "testSetDataBaseSettings", "org.postgresql.Driver",
                "jdbc:postgresql://127.0.0.1:5432/LoadPullPgMsPub", "ags", "");

        settingStorage.setDataBaseSettings(newBoneCPSettings);
        BoneCPSettings boneCPSettings = settingStorage
                .getDataBaseSettingsByName("testSetDataBaseSettings");
        assertEquals(
                "Ошибка при получение существующих настроек по умолчанию!",
                newBoneCPSettings, boneCPSettings);

        // Обновление настроек
        boneCPSettings.setMinConnectionsPerPartition(1);
        boneCPSettings.setMaxConnectionsPerPartition(2);
        boneCPSettings.setPartitionCount(3);
        boneCPSettings.setConnectionTimeoutInMs(4);
        boneCPSettings.setCloseConnectionWatchTimeoutInMs(5);
        settingStorage.setDataBaseSettings(boneCPSettings);
        BoneCPSettings updatedBoneCPSettings = settingStorage
                .getDataBaseSettingsByName("testSetDataBaseSettings");
        assertEquals("Ошибка при получение обновленных настроек!",
                updatedBoneCPSettings, boneCPSettings);

        // Обновление идентификатора
        boneCPSettings.setPoolId("testSetDataBaseSettings2");
        settingStorage.setDataBaseSettings(boneCPSettings);
        updatedBoneCPSettings = settingStorage
                .getDataBaseSettingsByName("testSetDataBaseSettings2");
        assertEquals("Ошибка при получение настроек с новым идентификатором!",
                updatedBoneCPSettings, boneCPSettings);

        // updatedBoneCPSettings =
        // settingStorage.getDataBaseSettingsByName("testSetDataBaseSettings");
        // assertNull("Существуют настройки со старым идентификатором!",
        // updatedBoneCPSettings);
    }

    /**
     * Тест таймаута при привышении максимального количества открытых соединений
     * 
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test
    public void testDelDataBaseSettings() throws ClassNotFoundException,
            SQLException {
        // Создание настроек
        BoneCPSettings newBoneCPSettings = new BoneCPSettingsModel(
                "testDelDataBaseSettings", "org.postgresql.Driver",
                "jdbc:postgresql://127.0.0.1:5432/LoadPullPgMsPub", "ags", "");

        settingStorage.setDataBaseSettings(newBoneCPSettings);
        BoneCPSettings boneCPSettings = settingStorage
                .getDataBaseSettingsByName("testDelDataBaseSettings");
        assertEquals(
                "Ошибка при получение существующих настроек по умолчанию!",
                newBoneCPSettings, boneCPSettings);

        // Удаляем настройки
        settingStorage.delDataBaseSettings(boneCPSettings);
        boneCPSettings = settingStorage
                .getDataBaseSettingsByName("testDelDataBaseSettings");
        assertNull("Существуют удаленные настройки!", boneCPSettings);

    }

}
