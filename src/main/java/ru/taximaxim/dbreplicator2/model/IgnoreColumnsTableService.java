package ru.taximaxim.dbreplicator2.model;

import org.hibernate.HibernateException;
import org.hibernate.Session;
import org.hibernate.SessionFactory;

public class IgnoreColumnsTableService {
    /**
     * Хранилище настроек
     */
    private SessionFactory sessionFactory;

    /**
     * @param sessionFactory
     */
    public IgnoreColumnsTableService(SessionFactory sessionFactory) {
        this.sessionFactory = sessionFactory;
    }

    /**
     * Получение экземпляра настроек задачи по идентификатору
     *
     * @param runnerId
     * @return
     */
    public IgnoreColumnsTableModel getIgnoreColumnsTable(int id) {
        Session session = sessionFactory.openSession();
        try {
            return  (IgnoreColumnsTableModel) session.get(IgnoreColumnsTableModel.class, id);
        } finally {
            session.close();
        }
    }

    public void setIgnoreColumnsTableModel (IgnoreColumnsTableModel ignore) {
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        try {
            session.saveOrUpdate(ignore);
            session.getTransaction().commit();
        } catch (HibernateException e) {
            session.getTransaction().rollback();
            throw e;
        } finally {
            session.close();
        }
    }

    public void delIgnoreColumnsTableModel (IgnoreColumnsTableModel ignore) {
        Session session = sessionFactory.openSession();
        session.beginTransaction();
        try {
            session.delete(ignore);
            session.getTransaction().commit();
        } catch (HibernateException e) {
            session.getTransaction().rollback();
            throw e;
        } finally {
            session.close();
        }
    }
}
