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

package ru.taximaxim.dbreplicator2.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

import ru.taximaxim.dbreplicator2.utils.Utils;

/**
 * Хранилище настроек именнованных соединений к HikariCP на основе Hibernate
 *
 * @author petrov_im
 */
public class HikariCPSettingsService {

    /**
     * Хранилище настроек
     */
    private SessionFactory sessionFactory;

    /**
     * Конструктор хранилища настроек в Hibernate
     *
     * @param sessionFactory
     *            - фабрика сессий Hibernate
     */
    public HikariCPSettingsService(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    /**
     * Получение настроек HikariCP по имени пула
     * 
     * @param poolName имя пула
     * @return
     */
    public HikariCPSettingsModel getDataBaseSettingsByName(String poolName) {
        Session session = sessionFactory.openSession();
        try {
            return (HikariCPSettingsModel) session.get(HikariCPSettingsModel.class, poolName);
        } finally {
            session.close();
        }
    }

    /**
     * Получение всех настроек HikariCP
     * 
     * @return
     */
    public Map<String, HikariCPSettingsModel> getDataBaseSettings() {
        Map<String, HikariCPSettingsModel> result = new HashMap<String, HikariCPSettingsModel>();

        Session session = sessionFactory.openSession();
        try {
            List<HikariCPSettingsModel> settingsList =
                    Utils.castList(HikariCPSettingsModel.class,
                            session.createCriteria(HikariCPSettingsModel.class).list());

            for (HikariCPSettingsModel settings : settingsList) {
                result.put(settings.getPoolId(), settings);
            }
        } finally {
            session.close();
        }

        return result;
    }

    /**
     * Обновление настроек пула HikariCP
     * 
     * @param dataBaseSettings новые настройки пула
     */
    public void setDataBaseSettings(HikariCPSettingsModel dataBaseSettings) {
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        try {
            session.saveOrUpdate(dataBaseSettings);
            session.getTransaction().commit();
        } catch (HibernateException e) {
            session.getTransaction().rollback();
            throw e;
        } finally {
            session.close();
        }
    }

    /**
     * Удаление настроек пула HikariCP
     * 
     * @param dataBaseSettings удаляемые настройки
     */
    public void delDataBaseSettings(HikariCPSettingsModel dataBaseSettings) {
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        try {
            session.delete(dataBaseSettings);
            session.getTransaction().commit();
        } catch (HibernateException e) {
            session.getTransaction().rollback();
            throw e;
        } finally {
            session.close();
        }
    }
}
