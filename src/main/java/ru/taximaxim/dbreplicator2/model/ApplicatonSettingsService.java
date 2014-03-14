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

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

/**
 * @author mardanov_rm
 *
 */
public class ApplicatonSettingsService {
    
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
    public ApplicatonSettingsService(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    
    /**
     * Получение значения ключа
     * @param key ключ
     * @return - значение ключа
     */
    public String getValue (String key) {
        Session session = sessionFactory.openSession();
        try {
            ApplicatonSettingsModel value = 
                    (ApplicatonSettingsModel) session.get(ApplicatonSettingsModel.class, key);
            if (value != null) {
                return value.getValue();
            }
            return null;
        } finally {
            session.close();
        }
    }

    /**
     * Изменение ключа
     * @param settings
     */
    public void setApplicatonSettings(ApplicatonSettingsModel settings) {
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        try {
            session.saveOrUpdate(settings);
            session.getTransaction().commit();
        } catch (HibernateException e) {
            session.getTransaction().rollback();
            throw e;
        } finally {
            session.close();
        }
    }

    /**
     * Удаление ключа
     * @param settings
     */
    public void delApplicatonSettings (ApplicatonSettingsModel settings) {
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        try {
            session.delete(settings);
            session.getTransaction().commit();
        } catch (HibernateException e) {
            session.getTransaction().rollback();
            throw e;
        } finally {
            session.close();
        }
    }
    
}
