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

package ru.taximaxim.dbreplicator2;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.hibernate.service.ServiceRegistryBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.cf.BoneCPConnectionsFactory;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsModel;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsService;
import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.model.TaskSettingsModel;

/**
 * Тест соединения с базой данных по протоколу TCP.
 * 
 * @author ags
 */
public class H2CreateSetting {

    protected static final Logger LOG = Logger.getLogger(H2CreateSetting.class);
    protected static SessionFactory sessionFactory;
    protected static Session session;

    private static ThreadPool threadPool = null;
    private static int count = 3;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

    }

    @AfterClass
    public static void setUpAfterClass() throws Exception {

    }

    @Test
    @Ignore   /// Не доделан
    public void testConnection() throws SQLException, ClassNotFoundException {

        sessionFactory = Application.getSessionFactory();
        
        session = sessionFactory.openSession();
        
        LOG.debug("Start: ");
        String poolName = "pull";
        String source = "source";
        BoneCPSettingsService cpSettingsService = new BoneCPSettingsService(sessionFactory);
        
        BoneCPSettingsModel settingsPool = new BoneCPSettingsModel(
                poolName, "org.h2.Driver", "jdbc:h2:mem://localhost/~/test", "sa", "");
        
        cpSettingsService.setDataBaseSettings(settingsPool);
        
        BoneCPSettingsModel settingsSource = new BoneCPSettingsModel(
                source,   "org.h2.Driver", "jdbc:h2:mem://localhost/~/test", "sa", "");
        
        cpSettingsService.setDataBaseSettings(settingsSource);


//        BoneCPConnectionsFactory connectionsFactory = new BoneCPConnectionsFactory(cpSettingsService);
//        
//        try {
//            connectionsFactory.getConnection(poolName);
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//            throw e;
//        } catch (SQLException e) {
//            e.printStackTrace();
//            throw e;
//        }
        
        
        RunnerModel runnerModel = createRunner(source, poolName, "description");

        StrategyModel strategyModel = createStrategy("ru.taximaxim.Class", null, true, 100);

        runnerModel.getStrategyModels().add(strategyModel);
        runnerModel.setStrategyModels(runnerModel.getStrategyModels());

        strategyModel.setRunner(runnerModel);

        
        BoneCPSettingsModel settings_orig = new BoneCPSettingsModel("test",
                "org.h2.Driver", "jdbc:h2:tcp://localhost:8084/~/H2Settings",
                "sa", "*****", 100, 50, 90, 70, 30);


//        TaskSettingsImpl taskSettingsImpl = new TaskSettingsImpl();
//        taskSettingsImpl.setDescription("pull");
//        taskSettingsImpl.setEnabled(true);
//        taskSettingsImpl.setFailInterval(1);
//        taskSettingsImpl.setPriority(5);
//        taskSettingsImpl.setReplicaId(10);
//        taskSettingsImpl.setSuccessInterval(20);
//        taskSettingsImpl.setTaskId(30);
//
//           
        delete(settingsSource);
        delete(settingsPool);
        add(settingsPool);
        add(settingsSource);
//        add(settings_orig);
//        //add(settings);
//        add(runnerModel);
//        add(strategyModel);
//        add(taskSettingsImpl);     
        threadPool = new ThreadPool(count);
        threadPool.start(runnerModel);
        threadPool.shutdown();
//        
//        LOG.info("TaskSettingsImpl: ");
//        for (TaskSettingsImpl tSettingsImpl : getAllTaskSettingsImpl()) {
//
//            Assert.assertEquals(taskSettingsImpl.getDescription(), tSettingsImpl.getDescription());
//            Assert.assertEquals(taskSettingsImpl.getEnabled(), tSettingsImpl.getEnabled());
//            Assert.assertEquals(taskSettingsImpl.getFailInterval(), tSettingsImpl.getFailInterval());
//            Assert.assertEquals(taskSettingsImpl.getPriority(), tSettingsImpl.getPriority());
//            Assert.assertEquals(taskSettingsImpl.getReplicaId(), tSettingsImpl.getReplicaId());
//            Assert.assertEquals(taskSettingsImpl.getSuccessInterval(), tSettingsImpl.getSuccessInterval());
//            Assert.assertEquals(taskSettingsImpl.getTaskId(), tSettingsImpl.getTaskId());
//            
//            
////            LOG.info(String.format("\nDescription: [%s]=[%s]"
////                    + "\nEnabled: [%s]=[%s]" 
////                    + "\nFailInterval: [%s]=[%s]"
////                    + "\nPriority: [%s]=[%s]" 
////                    + "\nReplicaId: [%s]=[%s]"
////                    + "\nSuccessInterval: [%s]=[%s]"
////                    + "\nTaskId: [%s]=[%s]"
////                    ,taskSettingsImpl.getDescription(), tSettingsImpl.getDescription()
////                    ,taskSettingsImpl.getEnabled(), tSettingsImpl.getEnabled()
////                    ,taskSettingsImpl.getFailInterval(), tSettingsImpl.getFailInterval()
////                    ,taskSettingsImpl.getPriority(), tSettingsImpl.getPriority()
////                    ,taskSettingsImpl.getReplicaId(), tSettingsImpl.getReplicaId()
////                    ,taskSettingsImpl.getSuccessInterval(), tSettingsImpl.getSuccessInterval()
////                    ,taskSettingsImpl.getTaskId(), tSettingsImpl.getTaskId()
////                    //
////                    ));
//        }
//        
//        LOG.info("StrategyModel: ");
//        for (StrategyModel strategy : getAllStrategyModel()) {
//
//            Assert.assertEquals(strategyModel.getClass(), strategy.getClass());
//            Assert.assertEquals(strategyModel.getClassName(), strategy.getClassName());
//            Assert.assertEquals(strategyModel.getId(), strategy.getId());
//            Assert.assertEquals(strategyModel.getParam(), strategy.getParam());
//            Assert.assertEquals(strategyModel.getPriority(),strategy.getPriority());
//            //Assert.assertEquals(strategyModel.getRunner(), strategy.getRunner());
//            
////            LOG.info(String.format("\nClassName: [%s]=[%s]"
////                    + "\nId: [%s]=[%s]"
////                    + "\nParam: [%s]=[%s]" 
////                    + "\nPriority: [%s]=[%s]"
////                    + "\nRunner: [%s]=[%s]"
////                    ,strategyModel.getClassName(), strategy.getClassName()
////                    ,strategyModel.getId(), strategy.getId()
////                    ,strategyModel.getParam(),strategy.getParam()
////                    ,strategyModel.getPriority(), strategy.getPriority()
////                    ,strategyModel.getRunner(), strategy.getRunner()
////                    //
////                    ));
//        }
//
//        LOG.info("RunnerModel: ");
//        for (RunnerModel rModel : getAllRunnerModel()) {
//            Assert.assertEquals(runnerModel.getClass(), rModel.getClass());
//            Assert.assertEquals(runnerModel.getDescription(), rModel.getDescription());
//            Assert.assertEquals(runnerModel.getId(), rModel.getId());
//            Assert.assertEquals(runnerModel.getSource(), rModel.getSource());
//            // Assert.assertEquals(runnerModel, rModel);
//            Assert.assertEquals(runnerModel.getTarget(), rModel.getTarget());
//            
////            LOG.info(String.format("\nDescription: [%s]=[%s]"
////                + "\nId: [%s]=[%s]"
////                + "\nSource: [%s]=[%s]"
////             //   + "\nStrategyModels: [%s]=[%s]"
////                + "\nTarget: [%s]=[%s]"
////                ,runnerModel.getDescription(), rModel.getDescription()
////                ,runnerModel.getId(),rModel.getId()
////                ,runnerModel.getSource(),rModel.getSource()
////             //   ,runnerModel.getStrategyModels(),rModel.getStrategyModels()
////                ,runnerModel.getTarget(),rModel.getTarget()
////             //
////                    ));
//        }
        
       LOG.info("Settings: ");
       Collection<BoneCPSettingsModel> str = getAllSettings();
       
       LOG.info("Settings: " + str.size());
        for (BoneCPSettingsModel setting_base : getAllSettings()) {

//            Assert.assertEquals(settings_orig.getPoolId(), setting_base.getPoolId());
//            Assert.assertEquals(settings_orig.getCloseConnectionWatchTimeoutInMs(), setting_base.getCloseConnectionWatchTimeoutInMs());
//            Assert.assertEquals(settings_orig.getConnectionTimeoutInMs(), setting_base.getConnectionTimeoutInMs());
//            Assert.assertEquals(settings_orig.getDriver(), setting_base.getDriver());
//            Assert.assertEquals(settings_orig.getMaxConnectionsPerPartition(), setting_base.getMaxConnectionsPerPartition());
//            Assert.assertEquals(settings_orig.getMinConnectionsPerPartition(), setting_base.getMinConnectionsPerPartition());
//            Assert.assertEquals(settings_orig.getPartitionCount(), setting_base.getPartitionCount());
//            Assert.assertEquals(settings_orig.getPass(), setting_base.getPass());
//            Assert.assertEquals(settings_orig.getUrl(), setting_base.getUrl());
//            Assert.assertEquals(settings_orig.getUser(), setting_base.getUser());
            
            LOG.info(String.format("\nCloseConnectionWatchTimeoutInMs: [%s]=[%s]"
                    + "\nConnectionTimeoutInMs: [%s]=[%s]"
                    + "\nDriver: [%s]=[%s]"
                    + "\nMaxConnectionsPerPartition: [%s]=[%s]"
                    + "\nMinConnectionsPerPartition: [%s]=[%s]"
                    + "\nPartitionCount: [%s]=[%s]"
                    + "\nPass: [%s]=[%s]"
                    + "\nPoolId: [%s]=[%s]" 
                    + "\nUrl: [%s]=[%s]" 
                    + "\nUser: [%s]=[%s]"
                    ,settings_orig.getCloseConnectionWatchTimeoutInMs(), setting_base.getCloseConnectionWatchTimeoutInMs()
                    ,settings_orig.getConnectionTimeoutInMs(), setting_base.getConnectionTimeoutInMs()
                    ,settings_orig.getDriver(), setting_base.getDriver()
                    ,settings_orig.getMaxConnectionsPerPartition(), setting_base.getMaxConnectionsPerPartition()
                    ,settings_orig.getMinConnectionsPerPartition(), setting_base.getMinConnectionsPerPartition()
                    ,settings_orig.getPartitionCount(), setting_base.getPartitionCount()
                    ,settings_orig.getPass(), setting_base.getPass()
                    ,settings_orig.getPoolId(), setting_base.getPoolId()
                    ,settings_orig.getUrl(), setting_base.getUrl()
                    ,settings_orig.getUser(), setting_base.getUser()
                    //
                    ));
        }
        
    }
    
    // /////////////////////////////////////////////////////////////////////////////////////////////

    public RunnerModel createRunner(String source, String target,
            String description) {

        RunnerModel runner = new RunnerModel();

        runner.setSource(source);
        runner.setTarget(target);
        runner.setDescription(description);

        return runner;
    }

    public StrategyModel createStrategy(String className, String param,
            boolean isEnabled, int priority) {

        StrategyModel strategy = new StrategyModel();

        strategy.setClassName(className);
        strategy.setParam(param);
        strategy.setEnabled(isEnabled);
        strategy.setPriority(priority);

        return strategy;
    }

    public void isSession() {
        if (!session.isOpen()) {
            session = sessionFactory.openSession();
        }
    }
    
    public void close() {
//        if (session != null && session.isOpen()) {
//            session.close();
//        }
    }
    // /////////////////////////////////////////////////////////////////////////////////////////////

    public void add(BoneCPSettingsModel objclass) throws SQLException {
        try {
            isSession();
            session.beginTransaction();
            session.save(objclass);
            session.getTransaction().commit();
        } catch (Exception ex) {
            LOG.error("Ошибка при вставке: " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
    }

    public void update(Long id, BoneCPSettingsModel objclass)
            throws SQLException {
        try {
            isSession();
            session.beginTransaction();
            session.update(objclass);
            session.getTransaction().commit();
        } catch (Exception ex) {
            LOG.error("Ошибка при обновление: " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
    }

    public BoneCPSettingsModel getSettingById(Long id) throws SQLException {
        BoneCPSettingsModel objclass = null;
        try {
            isSession();
            objclass = (BoneCPSettingsModel) session.load(
                    BoneCPSettingsModel.class, id);
        } catch (Exception ex) {
            LOG.error("Ошибка 'findById': " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
        return objclass;
    }

    @SuppressWarnings("unchecked")
    public Collection<BoneCPSettingsModel> getAllSettings() throws SQLException {
        List<BoneCPSettingsModel> objclass = new ArrayList<BoneCPSettingsModel>();
        try {
            isSession();
            objclass = session.createCriteria(BoneCPSettingsModel.class).list();
        } catch (Exception ex) {
            LOG.error("Ошибка 'getAll': " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
        return objclass;
    }

    public void delete(BoneCPSettingsModel objclass) throws SQLException {
        try {
            isSession();
            session.beginTransaction();
            session.delete(objclass);
            session.getTransaction().commit();
        } catch (Exception ex) {
            LOG.error("Ошибка при удалении: " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
    }

    // /////////////////////////////////////////////////////////////////////////////////////////////

    public void add(StrategyModel objclass) throws SQLException {
        try {
            isSession();
            session.beginTransaction();
            session.save(objclass);
            session.getTransaction().commit();
        } catch (Exception ex) {
            LOG.error("Ошибка при вставке: " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
    }

    public void update(Long id, StrategyModel objclass) throws SQLException {
        try {
            isSession();
            session.beginTransaction();
            session.update(objclass);
            session.getTransaction().commit();
        } catch (Exception ex) {
            LOG.error("Ошибка при обновление: " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
    }

    public StrategyModel getStrategyModelById(Long id) throws SQLException {
        StrategyModel objclass = null;
        try {
            isSession();
            objclass = (StrategyModel) session.load(StrategyModel.class, id);
        } catch (Exception ex) {
            LOG.error("Ошибка 'findById': " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
        return objclass;
    }

    @SuppressWarnings("unchecked")
    public Collection<StrategyModel> getAllStrategyModel() throws SQLException {
        List<StrategyModel> objclass = new ArrayList<StrategyModel>();
        try {
            isSession();
            objclass = session.createCriteria(StrategyModel.class).list();
        } catch (Exception ex) {
            LOG.error("Ошибка 'getAll': " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
        return objclass;
    }

    public void delete(StrategyModel objclass) throws SQLException {
        try {
            isSession();
            session.beginTransaction();
            session.delete(objclass);
            session.getTransaction().commit();
        } catch (Exception ex) {
            LOG.error("Ошибка при удалении: " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
    }

    // /////////////////////////////////////////////////////////////////////////////////////////////

    public void add(TaskSettingsModel objclass) throws SQLException {
        try {
            isSession();
            session.beginTransaction();
            session.save(objclass);
            session.getTransaction().commit();
        } catch (Exception ex) {
            LOG.error("Ошибка при вставке: " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
    }

    public void update(Long id, TaskSettingsModel objclass) throws SQLException {
        try {
            isSession();
            session.beginTransaction();
            session.update(objclass);
            session.getTransaction().commit();
        } catch (Exception ex) {
            LOG.error("Ошибка при обновление: " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
    }

    public TaskSettingsModel getTaskSettingsImplById(Long id)
            throws SQLException {
        TaskSettingsModel objclass = null;
        try {
            isSession();
            objclass = (TaskSettingsModel) session.load(TaskSettingsModel.class,
                    id);
        } catch (Exception ex) {
            LOG.error("Ошибка 'findById': " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
        return objclass;
    }

    @SuppressWarnings("unchecked")
    public Collection<TaskSettingsModel> getAllTaskSettingsImpl()
            throws SQLException {
        List<TaskSettingsModel> objclass = new ArrayList<TaskSettingsModel>();
        try {
            isSession();
            objclass = session.createCriteria(TaskSettingsModel.class).list();
        } catch (Exception ex) {
            LOG.error("Ошибка 'getAll': " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
        return objclass;
    }

    public void delete(TaskSettingsModel objclass) throws SQLException {
        try {
            isSession();
            session.beginTransaction();
            session.delete(objclass);
            session.getTransaction().commit();
        } catch (Exception ex) {
            LOG.error("Ошибка при удалении: " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
    }

    // /////////////////////////////////////////////////////////////////////////////////////////////

    public void add(RunnerModel objclass) throws SQLException {
        try {
            isSession();
            session.beginTransaction();
            session.save(objclass);
            session.getTransaction().commit();
        } catch (Exception ex) {
            LOG.error("Ошибка при вставке: " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
    }

    public void update(Long id, RunnerModel objclass) throws SQLException {
        try {
            isSession();
            session.beginTransaction();
            session.update(objclass);
            session.getTransaction().commit();
        } catch (Exception ex) {
            LOG.error("Ошибка при обновление: " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
    }

    public RunnerModel getRunnerModelById(Long id) throws SQLException {
        RunnerModel objclass = null;
        try {
            isSession();
            objclass = (RunnerModel) session.load(RunnerModel.class, id);
        } catch (Exception ex) {
            LOG.error("Ошибка 'findById': " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
        return objclass;
    }

    @SuppressWarnings("unchecked")
    public Collection<RunnerModel> getAllRunnerModel() throws SQLException {
        List<RunnerModel> objclass = new ArrayList<RunnerModel>();
        try {
            isSession();
            objclass = session.createCriteria(RunnerModel.class).list();
        } catch (Exception ex) {
            LOG.error("Ошибка 'getAll': " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
        return objclass;
    }

    public void delete(RunnerModel objclass) throws SQLException {
        try {
            isSession();
            session.beginTransaction();
            session.delete(objclass);
            session.getTransaction().commit();
        } catch (Exception ex) {
            LOG.error("Ошибка при удалении: " + ex.getMessage());
            ex.printStackTrace();
            throw new SQLException(ex);
        } finally {
            close();
        }
    }
}
