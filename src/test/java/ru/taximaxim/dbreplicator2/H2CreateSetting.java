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
import org.junit.Test;

import ru.taximaxim.dbreplicator2.model.BoneCPSettingsImpl;
import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.model.TaskSettingsImpl;

/**
 * Тест соединения с базой данных по протоколу TCP.
 * 
 * @author ags
 */
public class H2CreateSetting {

	protected static final Logger LOG = Logger.getLogger(H2CreateSetting.class);
	protected static SessionFactory sessionFactory;
	protected static Session session;
	
//	private static ThreadPool threadPool = null;
//	private static int count = 3;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {

	}

	@AfterClass
	public static void setUpAfterClass() throws Exception {

	}

	@Test
	public void testConnection() throws SQLException {
		Configuration configuration = new Configuration();
		configuration.configure();

		// http://stackoverflow.com/a/15702946/2743959
		ServiceRegistry serviceRegistry = new ServiceRegistryBuilder()
				.applySettings(configuration.getProperties())
				.buildServiceRegistry();
		sessionFactory = configuration.buildSessionFactory(serviceRegistry);

		session = sessionFactory.openSession();
		session.beginTransaction();
		
		RunnerModel runnerModel = createRunner("source", "target",
				"Описание исполняемого потока");
		
		
		StrategyModel strategyModel = createStrategy("ru.taximaxim.Class",
				null, true, 100);

		runnerModel.getStrategyModels().add(strategyModel);
		runnerModel.setStrategyModels(runnerModel.getStrategyModels());
		
		strategyModel.setRunner(runnerModel);
		
		LOG.debug("Settings: ");

		BoneCPSettingsImpl settings_orig = new BoneCPSettingsImpl("poolid",
				"org.h2.Driver", "jdbc:h2:tcp://localhost:8084/~/H2Settings",
				"sa", "*****", 100, 50, 90, 70, 30);

		add(settings_orig);
		
		for (BoneCPSettingsImpl setting_base : getAllSettings()) {

			Assert.assertEquals(settings_orig.getPoolId(),
					setting_base.getPoolId());
			Assert.assertEquals(
					settings_orig.getCloseConnectionWatchTimeoutInMs(),
					setting_base.getCloseConnectionWatchTimeoutInMs());
			Assert.assertEquals(settings_orig.getConnectionTimeoutInMs(),
					setting_base.getConnectionTimeoutInMs());
			Assert.assertEquals(settings_orig.getDriver(),
					setting_base.getDriver());
			Assert.assertEquals(settings_orig.getMaxConnectionsPerPartition(),
					setting_base.getMaxConnectionsPerPartition());
			Assert.assertEquals(settings_orig.getMinConnectionsPerPartition(),
					setting_base.getMinConnectionsPerPartition());
			Assert.assertEquals(settings_orig.getPartitionCount(),
					setting_base.getPartitionCount());
			Assert.assertEquals(settings_orig.getPass(), setting_base.getPass());
			Assert.assertEquals(settings_orig.getUrl(), setting_base.getUrl());
			Assert.assertEquals(settings_orig.getUser(), setting_base.getUser());
			
			LOG.info(String.format("\nPoolId: [%s]"
					+ "\nCloseConnectionWatchTimeoutInMs: [%s]"
					+ "\nConnectionTimeoutInMs: [%s]" + "\nDriver: [%s]"
					+ "\nMaxConnectionsPerPartition: [%s]"
					+ "\nMinConnectionsPerPartition: [%s]"
					+ "\nPartitionCount: [%s]" + "\nPass: [%s]" + "\nUrl: [%s]"
					+ "\nUser: [%s]", setting_base.getPoolId(),
					setting_base.getCloseConnectionWatchTimeoutInMs(),
					setting_base.getConnectionTimeoutInMs(),
					setting_base.getDriver(),
					setting_base.getMaxConnectionsPerPartition(),
					setting_base.getMinConnectionsPerPartition(),
					setting_base.getPartitionCount(), setting_base.getPass(),
					setting_base.getUrl(), setting_base.getUser()));
		}


		add(runnerModel);
		
		LOG.debug("RunnerModel: ");
		
		for (RunnerModel rModel : getAllRunnerModel()) {
			Assert.assertEquals(runnerModel.getClass(), rModel.getClass());
			Assert.assertEquals(runnerModel.getDescription(), rModel.getDescription());
			Assert.assertEquals(runnerModel.getId(), rModel.getId());
			Assert.assertEquals(runnerModel.getSource(), rModel.getSource());
			Assert.assertEquals(runnerModel.getTarget(), rModel.getTarget());
			
			LOG.info(String.format(
					  "\ngetClass: [%s]=[%s]"
					+ "\nDescription: [%s]=[%s]"
				    + "\nId: [%s]=[%s]" 
					+ "\nSource: [%s]=[%s]"
					+ "\nTarget: [%s]=[%s]"
					//+ "\nStrategyModels: [%s]=[%s]"
					,runnerModel.getClass(), rModel.getClass()
					,runnerModel.getDescription(), rModel.getDescription()
					,runnerModel.getId(), rModel.getId()
					,runnerModel.getSource(), rModel.getSource()
					,runnerModel.getTarget(), rModel.getTarget()
					//,runnerModel.getStrategyModels(), rModel.getStrategyModels()
					));
			//Assert.assertEquals(runnerModel, rModel);
		}
		
		add(strategyModel);
		
		LOG.debug("StrategyModel: ");
		
		for (StrategyModel strategy : getAllStrategyModel()) {
			
			Assert.assertEquals(strategyModel.getClass(), strategy.getClass());
			Assert.assertEquals(strategyModel.getClassName(), strategy.getClassName());
			Assert.assertEquals(strategyModel.getId(), strategy.getId());
			Assert.assertEquals(strategyModel.getParam(), strategy.getParam());
			Assert.assertEquals(strategyModel.getPriority(), strategy.getPriority());
			//Assert.assertEquals(strategyModel.getRunner(), strategy.getRunner());
			
			LOG.info(String.format(
					  "\ngetClass: [%s]=[%s]"
					+ "\nDescription: [%s]=[%s]"
				    + "\nId: [%s]=[%s]" 
					+ "\nParam: [%s]=[%s]"
					+ "\nPriority: [%s]=[%s]"
					+ "\nRunner: [%s]=[%s]"
					,strategyModel.getClass(), strategy.getClass()
					,strategyModel.getClassName(), strategy.getClassName()
					,strategyModel.getId(), strategy.getId()
					,strategyModel.getParam(), strategy.getParam()
					,strategyModel.getPriority(), strategy.getPriority()
					,strategyModel.getRunner(), strategy.getRunner()
					));
			//Assert.assertEquals(runnerModel, rModel);
		}
		
		
		
//		threadPool = new ThreadPool(count);
//		threadPool.start(runnerModel);
//		threadPool.shutdown();
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

	public void isSessionTransaction(){
		if(!session.isOpen()) {
			session = sessionFactory.openSession(); 
			session.beginTransaction();
		}
	}
	
	public void isSession(){
		if(!session.isOpen()) {
			session = sessionFactory.openSession(); 
		}
	}
	
	// /////////////////////////////////////////////////////////////////////////////////////////////

	public void add(BoneCPSettingsImpl objclass) throws SQLException {
		try {
			isSessionTransaction();
			session.save(objclass);
			session.getTransaction().commit();
		} catch (Exception ex) {
			LOG.error("Ошибка при вставке: " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
	}

	public void update(Long id, BoneCPSettingsImpl objclass)
			throws SQLException {
		try {
			isSessionTransaction();
			session.update(objclass);
			session.getTransaction().commit();
		} catch (Exception ex) {
			LOG.error("Ошибка при обновление: " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
	}

	public BoneCPSettingsImpl getSettingById(Long id) throws SQLException {
		BoneCPSettingsImpl objclass = null;
		try {
			isSession();
			objclass = (BoneCPSettingsImpl) session.load(
					BoneCPSettingsImpl.class, id);
		} catch (Exception ex) {
			LOG.error("Ошибка 'findById': " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
		return objclass;
	}

	@SuppressWarnings("unchecked")
	public Collection<BoneCPSettingsImpl> getAllSettings() throws SQLException {
		List<BoneCPSettingsImpl> objclass = new ArrayList<BoneCPSettingsImpl>();
		try {
			isSession(); 
			objclass = session.createCriteria(BoneCPSettingsImpl.class).list();
		} catch (Exception ex) {
			LOG.error("Ошибка 'getAll': " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
		return objclass;
	}

	public void delete(BoneCPSettingsImpl objclass) throws SQLException {
		try {
			isSessionTransaction();
			session.delete(objclass);
			session.getTransaction().commit();
		} catch (Exception ex) {
			LOG.error("Ошибка при удалении: " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
	}

	// /////////////////////////////////////////////////////////////////////////////////////////////

	public void add(StrategyModel objclass) throws SQLException {
		try {
			isSessionTransaction();
			session.save(objclass);
			session.getTransaction().commit();
		} catch (Exception ex) {
			LOG.error("Ошибка при вставке: " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
	}

	public void update(Long id, StrategyModel objclass) throws SQLException {
		try {
			isSessionTransaction();
			session.update(objclass);
			session.getTransaction().commit();
		} catch (Exception ex) {
			LOG.error("Ошибка при обновление: " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
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
			if (session != null && session.isOpen()) {
				session.close();
			}
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
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
		return objclass;
	}

	public void delete(StrategyModel objclass) throws SQLException {
		try {
			isSessionTransaction();
			session.delete(objclass);
			session.getTransaction().commit();
		} catch (Exception ex) {
			LOG.error("Ошибка при удалении: " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
	}

	// /////////////////////////////////////////////////////////////////////////////////////////////

	public void add(TaskSettingsImpl objclass) throws SQLException {
		try {
			isSessionTransaction();
			session.save(objclass);
			session.getTransaction().commit();
		} catch (Exception ex) {
			LOG.error("Ошибка при вставке: " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
	}

	public void update(Long id, TaskSettingsImpl objclass) throws SQLException {
		try {
			isSessionTransaction();
			session.update(objclass);
			session.getTransaction().commit();
		} catch (Exception ex) {
			LOG.error("Ошибка при обновление: " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
	}

	public TaskSettingsImpl getTaskSettingsImplById(Long id)
			throws SQLException {
		TaskSettingsImpl objclass = null;
		try {
			isSession(); 
			objclass = (TaskSettingsImpl) session.load(TaskSettingsImpl.class,
					id);
		} catch (Exception ex) {
			LOG.error("Ошибка 'findById': " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
		return objclass;
	}

	@SuppressWarnings("unchecked")
	public Collection<TaskSettingsImpl> getAllTaskSettingsImpl()
			throws SQLException {
		List<TaskSettingsImpl> objclass = new ArrayList<TaskSettingsImpl>();
		try {
			isSession(); 
			objclass = session.createCriteria(TaskSettingsImpl.class).list();
		} catch (Exception ex) {
			LOG.error("Ошибка 'getAll': " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
		return objclass;
	}

	public void delete(TaskSettingsImpl objclass) throws SQLException {
		try {
			isSessionTransaction();
			session.delete(objclass);
			session.getTransaction().commit();
		} catch (Exception ex) {
			LOG.error("Ошибка при удалении: " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
	}

	// /////////////////////////////////////////////////////////////////////////////////////////////

	public void add(RunnerModel objclass) throws SQLException {
		try {
			isSessionTransaction();
			session.save(objclass);
			session.getTransaction().commit();
		} catch (Exception ex) {
			LOG.error("Ошибка при вставке: " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
	}

	public void update(Long id, RunnerModel objclass) throws SQLException {
		try {
			isSessionTransaction();
			session.update(objclass);
			session.getTransaction().commit();
		} catch (Exception ex) {
			LOG.error("Ошибка при обновление: " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
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
			if (session != null && session.isOpen()) {
				session.close();
			}
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
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
		return objclass;
	}

	public void delete(RunnerModel objclass) throws SQLException {
		try {
			isSessionTransaction();
			session.delete(objclass);
			session.getTransaction().commit();
		} catch (Exception ex) {
			LOG.error("Ошибка при удалении: " + ex.getMessage());
			ex.printStackTrace();
			throw new SQLException(ex);
		} finally {
			if (session != null && session.isOpen()) {
				session.close();
			}
		}
	}
}
