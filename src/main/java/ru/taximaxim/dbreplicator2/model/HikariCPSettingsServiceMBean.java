package ru.taximaxim.dbreplicator2.model;

import java.util.Map;

import org.hibernate.SessionFactory;

public interface HikariCPSettingsServiceMBean {
    
    public HikariCPSettingsModel getDataBaseSettingsByName(String poolName);

    public Map<String, HikariCPSettingsModel> getDataBaseSettings();
    
    public void setDataBaseSettings(HikariCPSettingsModel dataBaseSettings);
    
    public void delDataBaseSettings(HikariCPSettingsModel dataBaseSettings);
    
    public SessionFactory getSessionFactory();

    public void setSessionFactory(SessionFactory sessionFactory);
}
