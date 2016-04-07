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

package ru.taximaxim.dbreplicator2.replica.strategies.superlog.algorithm;

import java.sql.SQLException;
import java.util.Collection;

import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.strategies.superlog.data.SuperlogDataService;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Класс стратегии менеджера записей суперлог таблицы с асинхронным параллельным
 * запуском обработчиков реплик
 * 
 * @author volodin_aa
 * 
 */
public class FastManagerAlgorithm extends GeneiricManagerAlgorithm {

    /**
     * Конструктор по умолчанию
     */
    public FastManagerAlgorithm(SuperlogDataService superlogDataService,
            StrategyModel data) {
        super(superlogDataService, data);
    }
    
    @Override
    protected void startRunners(Collection<RunnerModel> runners) throws SQLException  {
            // Асинхронно запускаем обработчики реплик
            for (RunnerModel runner : runners) {
                if (!getRunnersFromTask().contains(runner)) {
                    Core.getThreadPool().start(runner);
                }
            }
    }
}
