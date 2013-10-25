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

import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;

/**
 * Класс стратегии репликации данных из источника в приемник
 * 
 * @author volodin_aa
 * 
 */
public class ReplicationStrategy implements Strategy {

    /**
     * Конструктор по умолчанию
     */
    public ReplicationStrategy() {
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
            // Извлекаем список последних операций по измененым записям
            try (
                    PreparedStatement selectLastOperations = 
                        sourceConnection.prepareStatement("SELECT * FROM rep2_workpool_data WHERE (SELECT forei FROM rep2_workpool_data) id_runner=?");
            ) {
                selectWorkData.setInt(1, data.getId());
                try (ResultSet superLogResult = selectWorkData.executeQuery();) {
                    while (superLogResult.next()) {
                // Проходим по списку измененных записей
                // Реплицируем данные
                // Если была операция удаления, то удаляем запись в приемнике
                // Если Была операция вставки или изменения, то сначала пытаем обновить запись,
                // и если такой записи нет, то пытаемся вставить
                // Очищаем данные о текущей записи из набора данных реплики
                    }
            }
            // Подтверждаем транзакцию
            sourceConnection.commit();
            sourceConnection.setAutoCommit(lastAutoCommit);
        } catch (SQLException e) {
            throw new StrategyException(e);
        }
    }

}
