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

package ru.taximaxim.dbreplicator2.jmx.mbeans;

import java.util.Map;

import ru.taximaxim.dbreplicator2.model.HikariCPSettingsModel;
import ru.taximaxim.dbreplicator2.model.HikariCPSettingsService;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Mbean класс для передачи настроек репликатора через jmx
 * @author petrov_im
 *
 */
public class DbrepSettings implements DbrepSettingsMBean {

    @Override
    public HikariCPSettingsModel[] getHikariCPSettingsModels() {
        Map<String, HikariCPSettingsModel> hikariCPSettingsMap = new HikariCPSettingsService(Core.getSessionFactory()).getDataBaseSettings();
        HikariCPSettingsModel[] hikariCPSettingsArray = new HikariCPSettingsModel[hikariCPSettingsMap.keySet().size()];
        int i = 0;
        for (String key : hikariCPSettingsMap.keySet()) {
            hikariCPSettingsArray[i] = hikariCPSettingsMap.get(key);
            i++;
        }

        return hikariCPSettingsArray;
    }
}
