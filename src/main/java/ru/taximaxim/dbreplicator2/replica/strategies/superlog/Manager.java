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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.model.BoneCPSettingsModel;
import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.model.TableModel;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;

/**
 * Класс стратегии менеджера записей суперлог таблицы
 * 
 * @author volodin_aa
 * 
 */
public class Manager implements Strategy {

    private static final Logger LOG = Logger.getLogger(Manager.class);

    /**
     * Размер выборки данных (строк)
     */
    private int fetchSize = 1000;
    
    /**
     * Размер сбрасываемых в БД данных (строк)
     */
    private int batchSize = 1000;

    /**
     * Конструктор по умолчанию
     */
    public Manager() {
    }

    @Override
    public void execute(Connection sourceConnection, Connection targetConnection,
            StrategyModel data) throws StrategyException, SQLException {
        Boolean lastAutoCommit = null;
        Boolean lastTargetAutoCommit = null;
        try {
            lastAutoCommit = sourceConnection.getAutoCommit();
            lastTargetAutoCommit = targetConnection.getAutoCommit();
            // Начинаем транзакцию
            sourceConnection.setAutoCommit(false);
            sourceConnection
                .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            targetConnection.setAutoCommit(false);
            targetConnection
                .setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            // Строим список обработчиков реплик
            BoneCPSettingsModel sourcePool = data.getRunner().getSource();

            // Переносим данные
            try (
                    PreparedStatement insertRunnerData = 
                            targetConnection.prepareStatement("INSERT INTO rep2_workpool_data (id_runner, id_superlog, id_foreign, id_table, c_operation, c_date, id_transaction, c_errors_count) VALUES (?, ?, ?, ?, ?, ?, ?, 0)");
                    PreparedStatement deleteSuperLog = 
                            targetConnection.prepareStatement("DELETE FROM rep2_superlog WHERE id_superlog=?");
                    PreparedStatement selectSuperLog = 
                            sourceConnection.prepareStatement("SELECT * FROM rep2_superlog ORDER BY id_superlog");
            ) {
                selectSuperLog.setFetchSize(fetchSize);
                try (ResultSet superLogResult = selectSuperLog.executeQuery();) {
                    for (int rowsCount = 1; superLogResult.next(); rowsCount++) {
                        // Копируем записи
                        // Проходим по списку слушателей текущей таблицы
                        for (TableModel table : sourcePool.getTables()) {
                            if (table.getName()
                                    .equalsIgnoreCase(superLogResult.getString("id_table"))){
                                for (RunnerModel runner : table.getRunners()) {
                                    insertRunnerData.setInt(1,
                                            runner.getId());
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
                            }
                        }
                        // Удаляем исходную запись
                        deleteSuperLog.setLong(1, superLogResult.getLong("id_superlog"));
                        deleteSuperLog.addBatch();
                        
                        // Периодически сбрасываем батч в БД
                        if ((rowsCount % batchSize) == 0) {
                            insertRunnerData.executeBatch();
                            deleteSuperLog.executeBatch();
                            targetConnection.commit();
                            
                            LOG.info(String.format("Обработано %s строк...", rowsCount));
                        }
                    }
                    insertRunnerData.executeBatch();
                    deleteSuperLog.executeBatch();
                    // Подтверждаем транзакцию
                    targetConnection.commit();
                }
            } catch (SQLException e) {
                // Откатываемся
                sourceConnection.rollback();
                // Пробрасываем ошибку на уровень выше
                throw e;
            }
            // Запускаем обработчики реплик
            for (RunnerModel runner : sourcePool.getRunners()) {
                // Пока синхронный запуск!
                if (!runner.getTables().isEmpty()) {
                    WorkerThread workerThread = new WorkerThread(runner);
                    workerThread.run();
                }
            }
        } finally {
            try {
                if (lastAutoCommit != null) {
                    sourceConnection.setAutoCommit(lastAutoCommit);
                }
            } catch(SQLException e){
                // Ошибка может возникнуть если во время операции упало соединение к БД
                LOG.warn("Ошибка при возврате автокомита в исходное состояние.", e);
            }

            try {
                if (lastTargetAutoCommit != null) {
                    targetConnection.setAutoCommit(lastTargetAutoCommit);
                }
            } catch(SQLException sqlException){
                // Ошибка может возникнуть если во время операции упало соединение к БД
                LOG.warn("Ошибка при возврате автокомита в исходное состояние.", sqlException);
            }
        }
    }

}
