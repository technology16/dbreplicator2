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
 * Сервис для работы с настройками задач по расписанию 
 * 
 * @author volodin_aa
 *
 */
public class CronSettingsService {

    /**
     * Хранилище настроек
     */
    private SessionFactory sessionFactory;

    /**
     * @param sessionFactory
     */
    public CronSettingsService(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    /**
     * Возвращает список задач.
     *
     * @return
     */
    public Map<Integer, CronSettings> getTasks() {
        Map<Integer, CronSettings> result = new HashMap<Integer, CronSettings>();

        Session session = sessionFactory.openSession();
        try {
            List<CronSettings> settingsList =
                    Utils.castList(CronSettings.class,
                            session.createCriteria(CronSettingsModel.class).list());

            for (CronSettings task: settingsList){
                result.put(task.getTaskId(), task);
            }
        } finally {
            session.close();
        }

        return result;
    }

    /**
     * Получение экземпляра настроек задачи по идентификатору
     *
     * @param taskId
     * @return
     */
    public CronSettings getTask(int taskId) {
        Session session = sessionFactory.openSession();
        try {
            return  (CronSettings) session.get(CronSettingsModel.class, taskId);
        } finally {
            session.close();
        }
    }

    /**
     * Сохранение экземпляра настроек задачи
     *
     * @param CronSettings
     */
    public void setTask(CronSettings CronSettings) {
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        try {
            session.saveOrUpdate(CronSettings);
            session.getTransaction().commit();
        } catch (HibernateException e) {
            session.getTransaction().rollback();
            throw e;
        } finally {
            session.close();
        }
    }
}
