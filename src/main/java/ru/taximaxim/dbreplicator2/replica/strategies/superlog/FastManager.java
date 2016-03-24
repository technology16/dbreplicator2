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

package ru.taximaxim.dbreplicator2.replica.strategies.superlog;

import java.sql.Connection;
import java.sql.SQLException;

import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.StrategySkeleton;
import ru.taximaxim.dbreplicator2.replica.strategies.superlog.algorithm.FastManagerAlgorithm;
import ru.taximaxim.dbreplicator2.replica.strategies.superlog.data.GenericSuperlogDataService;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Класс стратегии менеджера записей суперлог таблицы с асинхронным параллельным
 * запуском обработчиков реплик
 * 
 * @author petrov_im
 *
 */
public class FastManager extends StrategySkeleton implements Strategy {

    @Override
    public void execute(Connection sourceConnection, Connection targetConnection,
            StrategyModel data)
            throws StrategyException, SQLException, ClassNotFoundException {

        try (Connection deleteConnection = Core.getConnectionFactory()
                .getConnection(data.getRunner().getSource().getPoolId());
                GenericSuperlogDataService superlogDataServise = new GenericSuperlogDataService(
                        sourceConnection, deleteConnection, targetConnection,
                        getFetchSize(data))) {
            new FastManagerAlgorithm(superlogDataServise).execute(data);
        }
    }
}
