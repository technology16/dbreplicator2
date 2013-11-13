package ru.taximaxim.dbreplicator2.model;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Restrictions;

import ru.taximaxim.dbreplicator2.utils.Utils;

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
    
    
    public List<IgnoreColumnsTableModel> getIgnoreList(int idTable) {
        Session session = sessionFactory.openSession();
        try {
            return Utils.castList(IgnoreColumnsTableModel.class,
                            session.createCriteria(IgnoreColumnsTableModel.class, "ignore_columns_table")
                            .list());
        } finally {
            session.close();
        }
    }
    
}
