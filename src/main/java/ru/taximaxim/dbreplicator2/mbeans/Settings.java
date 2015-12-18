package ru.taximaxim.dbreplicator2.mbeans;

import ru.taximaxim.dbreplicator2.model.TaskSettingsService;
import ru.taximaxim.dbreplicator2.utils.Core;

public class Settings implements SettingsMBean {

    @Override
    public TaskSettingsService getTaskSettingsService() {
        return Core.getTaskSettingsService();
    }

    @Override
    public int intValue() {
        return 555;
    }

    @Override
    public String stringValue() {
        return "value";
    }

}
