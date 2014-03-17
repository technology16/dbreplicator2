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

package ru.taximaxim.dbreplicator2.replica.strategies.errors;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.el.ErrorsLog;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.StrategySkeleton;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.algorithms.IntegrityReplicatedGenericAlgorithm;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.data.GenericDataTypeService;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.workpool.DelayGenericWorkPoolService;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * @author mardanov_rm
 */
public class IntegrityReplicatedData extends StrategySkeleton implements Strategy {
    public static final Logger LOG = Logger.getLogger(IntegrityReplicatedData.class);
    private static final String PERIOD = "period";
    private static final int DEFAULT_PERIOD = 300000;
    
    /**
     * Конструктор по умолчанию
     */
    public IntegrityReplicatedData() {
    }
    
    @Override
    public void execute(Connection sourceConnection, Connection targetConnection, StrategyModel data) 
            throws StrategyException, SQLException, ClassNotFoundException {
        int period = DEFAULT_PERIOD;
        if (data.getParam(PERIOD) != null) {
            period = Integer.parseInt(data.getParam(PERIOD));
        }
        
        try (ErrorsLog errorsLog = Core.getErrorsLog();
                DelayGenericWorkPoolService workPoolService = 
                        new DelayGenericWorkPoolService(sourceConnection, errorsLog, period);
                GenericDataTypeService genericDataServiceSourceConnection = 
                        new GenericDataTypeService(sourceConnection);
                GenericDataTypeService genericDataServiceTargetConnection = 
                        new GenericDataTypeService(targetConnection);) {
            IntegrityReplicatedGenericAlgorithm strategy = 
                    new IntegrityReplicatedGenericAlgorithm(
                            getFetchSize(data), 
                            getBatchSize(data), 
                            false, 
                            workPoolService, 
                            genericDataServiceSourceConnection, 
                            genericDataServiceTargetConnection);
               strategy.execute(sourceConnection, targetConnection, data);
           }
    }
}