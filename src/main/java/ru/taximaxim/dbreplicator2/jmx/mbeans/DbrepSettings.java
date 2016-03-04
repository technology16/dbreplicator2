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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import ru.taximaxim.dbreplicator2.model.ApplicatonSettingsModel;
import ru.taximaxim.dbreplicator2.model.ApplicatonSettingsService;
import ru.taximaxim.dbreplicator2.model.HikariCPSettingsModel;
import ru.taximaxim.dbreplicator2.model.HikariCPSettingsService;
import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.model.RunnerService;
import ru.taximaxim.dbreplicator2.model.TaskSettings;
import ru.taximaxim.dbreplicator2.model.TaskSettingsModel;
import ru.taximaxim.dbreplicator2.model.TaskSettingsService;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Mbean класс для передачи настроек репликатора через jmx
 * @author petrov_im
 *
 */
public class DbrepSettings implements DbrepSettingsMBean {

    @Override
    public HikariCPSettingsModel[] getHikariCPSettingsModels() {
        Map<String, HikariCPSettingsModel> hikariCPSettings = new HikariCPSettingsService(Core.getSessionFactory()).getDataBaseSettings();
        HikariCPSettingsModel[] hikariCPSettingsArray = new HikariCPSettingsModel[hikariCPSettings.size()];
        int i = 0;
        for (Entry<String, HikariCPSettingsModel> entry : hikariCPSettings.entrySet()) {
            hikariCPSettingsArray[i] = entry.getValue();
            i++;
        }
        return hikariCPSettingsArray;
    }

    @Override
    public ApplicatonSettingsModel[] getApplicatonSettingsModels() {
        List<ApplicatonSettingsModel> appSettings = new ApplicatonSettingsService(Core.getSessionFactory()).getApplicatonSettings();
        ApplicatonSettingsModel[] appSettingsArray = new ApplicatonSettingsModel[appSettings.size()];
        int i = 0;
        for (ApplicatonSettingsModel setting : appSettings) {
            appSettingsArray[i] = setting;
            i++;
        }
        return appSettingsArray;
    }

    @Override
    public RunnerModel[] getRunnerModels() {
        List<RunnerModel> runnerSettings = new RunnerService(Core.getSessionFactory()).getRunners();
        RunnerModel[] runnerSettingsArray = new RunnerModel[runnerSettings.size()];
        int i = 0;
        for (RunnerModel setting : runnerSettings) {
            runnerSettingsArray[i] = setting;
            i++;
        }
        return runnerSettingsArray;
    }

    @Override
    public TaskSettingsModel[] getTaskSettingsModels() {
        Map<Integer, TaskSettings> taskSettings = new TaskSettingsService(Core.getSessionFactory()).getTasks();
        TaskSettingsModel[] taskSettingsArray = new TaskSettingsModel[taskSettings.size()];
        int i = 0;
        for (Entry<Integer, TaskSettings> entry : taskSettings.entrySet()) {
            taskSettingsArray[i] = (TaskSettingsModel) entry.getValue();
            i++;
        }
        return taskSettingsArray;
    }
}
