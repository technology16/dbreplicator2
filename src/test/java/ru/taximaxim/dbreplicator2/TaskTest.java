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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractThirdTest;
import ru.taximaxim.dbreplicator2.model.BoneCPSettings;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsModel;
/**
 * Класс для тестирования пулов соединений
 * 
 * @author volodin_aa
 *
 */
public class TaskTest extends AbstractThirdTest  {

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
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test 
    public void testGetTask() throws ClassNotFoundException, SQLException {
        // Полуение несуществующих настроек
        BoneCPSettingsModel boneCPSettings = settingStorage.getDataBaseSettingsByName("testGetDataBaseSettingsByName");
        assertNull("Ошибка при получение несуществующих настроек!", boneCPSettings);
        
        // Получение существующих
        BoneCPSettingsModel newBoneCPSettings = new BoneCPSettingsModel("testGetDataBaseSettingsByName", 
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test", 
                "sa", 
                "");
        
        settingStorage.setDataBaseSettings(newBoneCPSettings);
        boneCPSettings = settingStorage.getDataBaseSettingsByName("testGetDataBaseSettingsByName");
        assertEquals("Ошибка при получение существующих настроек по умолчанию!", newBoneCPSettings, boneCPSettings);
        
        // Получение существующих
        newBoneCPSettings = new BoneCPSettingsModel("testGetDataBaseSettingsByName2", 
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test", 
                "sa", 
                "",
                1,
                2,
                3,
                4,
                5);
        
        settingStorage.setDataBaseSettings(newBoneCPSettings);
        boneCPSettings = settingStorage.getDataBaseSettingsByName("testGetDataBaseSettingsByName2");
        assertEquals("Ошибка при получение существующих настроек!", newBoneCPSettings, boneCPSettings);
        
        boneCPSettings = settingStorage.getDataBaseSettingsByName("testGetDataBaseSettingsByName");
        assertFalse("Ошибка при получение существующих настроек!", newBoneCPSettings.equals(boneCPSettings));
    }

    /** 
     * Тест получение всех настроек
     * 
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test 
    public void testGetTasks() throws ClassNotFoundException, SQLException {
        // Создание настроек
        BoneCPSettingsModel newBoneCPSettings1 = new BoneCPSettingsModel("testGetDataBaseSettings1", 
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test", 
                "sa", 
                "");
        settingStorage.setDataBaseSettings(newBoneCPSettings1);
        
        BoneCPSettingsModel newBoneCPSettings2 = new BoneCPSettingsModel("testGetDataBaseSettings2", 
                "org.postgresql.Driver", 
                "jdbc:postgresql://127.0.0.1:5432/LoadPullPgMsPub", 
                "ags", 
                "",
                1,
                2,
                3,
                4,
                5);
        
        settingStorage.setDataBaseSettings(newBoneCPSettings2);
        
        Map<String, BoneCPSettingsModel> settingsMap = settingStorage.getDataBaseSettings();
        assertEquals("Ошибка при получение существующих настроек по умолчанию!", 
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
    public void testSetTask() throws ClassNotFoundException, SQLException {
        // Создание настроек
        BoneCPSettingsModel newBoneCPSettings = new BoneCPSettingsModel("testSetDataBaseSettings", 
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test", 
                "sa", 
                "");
        
        settingStorage.setDataBaseSettings(newBoneCPSettings);
        BoneCPSettingsModel boneCPSettings = settingStorage.getDataBaseSettingsByName("testSetDataBaseSettings");
        assertEquals("Ошибка при получение существующих настроек по умолчанию!", newBoneCPSettings, boneCPSettings);
        
        // Обновление настроек
        boneCPSettings.setMinConnectionsPerPartition(1);
        boneCPSettings.setMaxConnectionsPerPartition(2);
        boneCPSettings.setPartitionCount(3);
        boneCPSettings.setConnectionTimeoutInMs(4);
        boneCPSettings.setCloseConnectionWatchTimeoutInMs(5);
        settingStorage.setDataBaseSettings(boneCPSettings);
        BoneCPSettings updatedBoneCPSettings = settingStorage.getDataBaseSettingsByName("testSetDataBaseSettings");
        assertEquals("Ошибка при получение обновленных настроек!", updatedBoneCPSettings, boneCPSettings);
        
        // Обновление идентификатора
        boneCPSettings.setPoolId("testSetDataBaseSettings2");
        settingStorage.setDataBaseSettings(boneCPSettings);
        updatedBoneCPSettings = settingStorage.getDataBaseSettingsByName("testSetDataBaseSettings2");
        assertEquals("Ошибка при получение настроек с новым идентификатором!", updatedBoneCPSettings, boneCPSettings);
        
        //updatedBoneCPSettings = settingStorage.getDataBaseSettingsByName("testSetDataBaseSettings");
        //assertNull("Существуют настройки со старым идентификатором!", updatedBoneCPSettings);
    }

    /** 
     * Тест таймаута при привышении максимального количества открытых соединений
     * 
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    @Test 
    public void testDelTask() throws ClassNotFoundException, SQLException {
        // Создание настроек
        BoneCPSettingsModel newBoneCPSettings = new BoneCPSettingsModel("testDelDataBaseSettings", 
                "org.h2.Driver",
                "jdbc:h2:mem://localhost/~/test", 
                "sa", 
                "");
        
        settingStorage.setDataBaseSettings(newBoneCPSettings);
        BoneCPSettingsModel boneCPSettings = settingStorage.getDataBaseSettingsByName("testDelDataBaseSettings");
        assertEquals("Ошибка при получение существующих настроек по умолчанию!", newBoneCPSettings, boneCPSettings);
        
        // Удаляем настройки
        settingStorage.delDataBaseSettings(boneCPSettings);
        boneCPSettings = settingStorage.getDataBaseSettingsByName("testDelDataBaseSettings");
        assertNull("Существуют удаленные настройки!", boneCPSettings);

    }
    
}
