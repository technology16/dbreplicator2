package ru.taximaxim.dbreplicator2.mbeans;

import ru.taximaxim.dbreplicator2.model.TaskSettingsService;

public interface SettingsMBean {
    
    public TaskSettingsService getTaskSettingsService();
    
    public int intValue();
    
    public String stringValue();
}