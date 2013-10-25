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

import java.util.List;
import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;

import ru.taximaxim.dbreplicator2.utils.Utils;

public class RunnerService {

    /**
     * Хранилище настроек
     */
    private SessionFactory sessionFactory;

    /**
     * @param sessionFactory
     */
    public RunnerService(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    /**
     * Возвражает список обработчиков.
     *
     * @return
     */
    public List<RunnerModel> getRunners() {
        Session session = sessionFactory.openSession();
        try {
            return Utils.castList(RunnerModel.class,
                            session.createCriteria(RunnerModel.class).list());
        } finally {
            session.close();
        }
    }

    /**
     * Возвражает список обработчиков одного класса.
     *
     * @return
     */
    public List<RunnerModel> getRunners(String className) {
        Session session = sessionFactory.openSession();
        try {
            return Utils.castList(RunnerModel.class,
                            session.createCriteria(RunnerModel.class)
                            .add(Restrictions.eq("class_name", className))
                            .list());
        } finally {
            session.close();
        }
    }

    /**
     * Получение экземпляра настроек задачи по идентификатору
     *
     * @param runnerId
     * @return
     */
    public RunnerModel getRunner(int runnerId) {
        Session session = sessionFactory.openSession();
        try {
            return  (RunnerModel) session.get(RunnerModel.class, runnerId);
        } finally {
            session.close();
        }
    }

    /**
     * Сохранение экземпляра настроек задачи
     *
     * @param runner
     */
    public void setRunner(RunnerModel runner) {
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        try {
            session.saveOrUpdate(runner);
            session.getTransaction().commit();
        } catch (HibernateException e) {
            session.getTransaction().rollback();
            throw e;
        } finally {
            session.close();
        }
    }
}
