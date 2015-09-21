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

import static org.junit.Assert.*;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractSettingTest;
import ru.taximaxim.dbreplicator2.model.BoneCPSettingsModel;
import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.model.StrategyModel;

public class StrategiesTest extends AbstractSettingTest {

    protected static final Logger LOG = Logger.getLogger(StrategiesTest.class);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp(null, null, null);
    }
    
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        close();
    }
    
    /**
     * Проверка создания модели исполняемого потока и стратегии.
     */
    @Test
    public void test() {

        session.beginTransaction();

        RunnerModel runner = createRunner(new BoneCPSettingsModel(), new BoneCPSettingsModel(), "Описание исполняемого потока");
        StrategyModel strategy = createStrategy(1, "ru.taximaxim.Class", "key", "value", true, 100);

        Assert.assertNull(runner.getId());
        LOG.debug("Идентификатор потока перед сохранением: " + runner.getId());

        addStrategy(runner, strategy);
        session.saveOrUpdate(runner);
        session.saveOrUpdate(strategy);

        LOG.debug("Идентификатор потока после его сохранения: " + runner.getId());
        Assert.assertNotNull(runner.getId());

        RunnerModel runner_compare = (RunnerModel) session.get(RunnerModel.class, runner.getId());

        LOG.debug("Идентификатор потока после его восстановления: " + runner_compare.getId());
        Assert.assertEquals(runner.getId(), runner_compare.getId());

        RunnerModel runner2 = createRunner(new BoneCPSettingsModel(), new BoneCPSettingsModel(), "Описание исполняемого потока (2)");
        StrategyModel strategy2 = createStrategy(2, "ru.taximaxim.Class", "key", "value", true, 100);
        addStrategy(runner2, strategy2);

        session.saveOrUpdate(runner2);
        session.saveOrUpdate(strategy2);
        
        Assert.assertNotNull(((StrategyModel) runner2.getStrategyModels().get(0)).getId());
        Assert.assertTrue("Нарушение описания состаного ключа стратегии", runner2.getStrategyModels().get(0).getRunner().equals(runner2));
        
        StrategyModel strategy3 = createStrategy(3, "ru.taximaxim.Class", "key", "value", true, 100);
        addStrategy(runner2, strategy3);

        session.saveOrUpdate(runner2);
        session.saveOrUpdate(strategy3);
        
        Assert.assertNotNull(((StrategyModel) runner2.getStrategyModels().get(1)).getId());
        Assert.assertTrue("Нарушение описания состаного ключа стратегии", runner2.getStrategyModels().get(1).getRunner().equals(runner2));
        
    }

    public RunnerModel createRunner(BoneCPSettingsModel source, BoneCPSettingsModel target, String description) {

        RunnerModel runner = new RunnerModel();

        runner.setSource(source);
        runner.setTarget(target);
        runner.setDescription(description);

        return runner;
    }

    public StrategyModel createStrategy(Integer id, String className, String key, String value, boolean isEnabled, int priority) {

        StrategyModel strategy = new StrategyModel();

        strategy.setId(id);
        strategy.setClassName(className);
        strategy.setParam(key, value);
        strategy.setEnabled(isEnabled);
        strategy.setPriority(priority);

        return strategy;
    }

    public void addStrategy(RunnerModel runner, StrategyModel strategy) {

        runner.getStrategyModels().add(strategy);
        strategy.setRunner(runner);

    }
    
    /**
     * Тестирование работы механизма хранения многострочных параметров    
     */
    @Test
    public void paramTest(){
        StrategyModel strategy = new StrategyModel();

        // Добавляем 1 параметр
        strategy.setParam("1", "11");
        
        // Добавляем 2 параметр
        strategy.setParam("2", "22");
        
        // Проверяем оба параметра
        assertTrue("Ошибка в 1 параметре!", strategy.getParam("1").equals("11"));
        assertTrue("Ошибка в 2 параметре!", strategy.getParam("2").equals("22"));
    }
}
