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

package ru.taximaxim.dbreplicator2.replica.strategies;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.model.RunnerService;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;

/**
 * Класс стратегии менеджера записей суперлог таблицы
 * 
 * @author volodin_aa
 * 
 */
public class SuperLogManagerStrategy implements Strategy {

    private RunnerService runnerService;
    
    /**
     * 
     */
    public SuperLogManagerStrategy(RunnerService runnerService) {
        this.runnerService = runnerService;
    }

    @Override
    public void execute(Connection sourceConnection, Connection targetConnection,
            StrategyModel data) throws StrategyException {
        try {
            boolean lastAutoCommit = sourceConnection.getAutoCommit();
            // Начинаем транзакцию
            sourceConnection.setAutoCommit(false);
            sourceConnection
                    .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            sourceConnection.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
            // Строим список обработчиков реплик
            List<RunnerModel> runners = new ArrayList<RunnerModel>();
            // Переносим данные
            try (
                 PreparedStatement insertRunnerData = sourceConnection.prepareStatement("INSERT INTO workpool_data (id_runner, id_superlog, id_foreign, id_table, c_operation, c_date, id_transaction) VALUES (?, ?, ?, ?, ?, ?, ?)");
                 PreparedStatement deleteSuperLog = sourceConnection.prepareStatement("DELETE FROM rep2_superlog WHERE id_superlog=?");
                 PreparedStatement selectSuperLog = sourceConnection.prepareStatement("SELECT * FROM rep2_superlog ORDER BY superlog_id");
             ) {
                selectSuperLog.setFetchSize(1000);
                try (ResultSet superLogResult = selectSuperLog.executeQuery();) {
                    while (superLogResult.next()) {
                        // Копируем записи
                        for (RunnerModel runner : runners) {
                            insertRunnerData.setInt(1,
                                    superLogResult.getInt(runner.getId()));
                            insertRunnerData.setLong(2,
                                    superLogResult.getLong("id_superlog"));
                            insertRunnerData.setInt(3,
                                    superLogResult.getInt("id_foreign"));
                            insertRunnerData.setString(4,
                                    superLogResult.getString("id_table"));
                            insertRunnerData.setString(5,
                                    superLogResult.getString("c_operation"));
                            insertRunnerData.setTimestamp(6,
                                    superLogResult.getTimestamp("c_date"));
                            insertRunnerData.setString(7,
                                    superLogResult.getString("id_transaction"));
                            insertRunnerData.addBatch();
                        }
                        // Удаляем исходную запись
                        deleteSuperLog.setLong(1, superLogResult.getLong("id_superlog"));
                        deleteSuperLog.addBatch();
                    }
                    insertRunnerData.executeBatch();
                    deleteSuperLog.executeBatch();
                }
            }
            // Подтверждаем транзакцию
            sourceConnection.commit();
            sourceConnection.setAutoCommit(lastAutoCommit);
            // Запускаем обработчики реплик
        } catch (SQLException e) {
            throw new StrategyException(e);
        }
    }

}
