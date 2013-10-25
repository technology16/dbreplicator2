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

import ru.taximaxim.dbreplicator2.Utils;
import ru.taximaxim.dbreplicator2.cf.BoneCPDataBaseSettingsStorage;

/**
 * Хранилище настроек именнованных соединений к BoneCP на основе Hibernate
 *
 * @author volodin_aa
 */
public class BoneCPSettingsService implements BoneCPDataBaseSettingsStorage {

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
    public BoneCPSettingsService(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    @Override
    public BoneCPSettings getDataBaseSettingsByName(String poolName) {
        Session session = sessionFactory.openSession();
        try {
            return (BoneCPSettings) session.get(BoneCPSettingsModel.class, poolName);
        } finally {
            session.close();
        }
    }

    @Override
    public Map<String, BoneCPSettings> getDataBaseSettings() {
        Map<String, BoneCPSettings> result = new HashMap<String, BoneCPSettings>();

        Session session = sessionFactory.openSession();
        try {
            List<BoneCPSettings> settingsList =
                    Utils.castList(BoneCPSettings.class,
                            session.createCriteria(BoneCPSettingsModel.class).list());

            for (BoneCPSettings settings : settingsList) {
                result.put(settings.getPoolId(), settings);
            }
        } finally {
            session.close();
        }

        return result;
    }

    @Override
    public void setDataBaseSettings(BoneCPSettings dataBaseSettings) {
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

    @Override
    public void delDataBaseSettings(BoneCPSettings dataBaseSettings) {
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
