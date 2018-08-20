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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.jdbc.Jdbc;
import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.strategies.replication.StrategySkeleton;

/**
 * Класс стратегии репликации данных из источника в приемник Записи
 * реплицируются в порядке последних операций над ними.
 * 
 * @author volodin_aa
 * 
 */
public class CountWatchgdog extends StrategySkeleton implements Strategy {

    private static final Logger LOG = Logger.getLogger(CountWatchgdog.class);

    private static final int DEFAULT_PART_EMAIL = 10;

    private static final String PART_EMAIL = "partEmail";
    private static final String COUNT = "count";

    @Override
    public void execute(ConnectionFactory connectionsFactory, StrategyModel data)
            throws SQLException {
        int partEmail = DEFAULT_PART_EMAIL;
        if (data.getParam(PART_EMAIL) != null) {
            partEmail = Integer.parseInt(data.getParam(PART_EMAIL));
        }

        String where = getWhere(data);

        if ((where == null)) {
            // Заменяем null пустой строкой. Что бы не мешал.
            where = "";
        }

        if (!where.isEmpty()) {
            // Если присутствует условие, то заворачиваем в его в скобки и
            // потом добавляем в запрос
            where = "AND (" + where + ")";
        }

        // Проверияем количество ошибочных итераций
        long rowCount = 0;
        try (Connection sourceConnection = connectionsFactory
                .get(data.getRunner().getSource().getPoolId()).getConnection();) {
            try (PreparedStatement selectErrorsCount = sourceConnection.prepareStatement(
                    "SELECT COUNT(*) as count FROM public.rep2_errors_log WHERE c_status = 0 "
                            + where)) {
                try (ResultSet countResult = selectErrorsCount.executeQuery();) {
                    if (countResult.next()) {
                        rowCount = countResult.getLong(COUNT);
                    }
                }
            }

            // Если нет ошибок то смысл в запуске данного кода бессмыслен
            if (rowCount != 0) {
                try (PreparedStatement selectErrors = sourceConnection.prepareStatement(
                        "SELECT t1.id_runner, t1.id_table, t1.id_foreign, t1.max_id_errors_log, t1.count, c_error, c_date FROM ( "
                                + "SELECT id_runner, id_table, id_foreign, MAX(id_errors_log) AS max_id_errors_log, COUNT(*) AS count "
                                + "FROM public.rep2_errors_log WHERE c_status = 0 "
                                + where
                                + " GROUP BY id_runner, id_table, id_foreign) as t1 "
                                + "LEFT JOIN rep2_errors_log ON t1.max_id_errors_log=rep2_errors_log.id_errors_log "
                                + "ORDER BY max_id_errors_log "
                                + "LIMIT ?")) {

                    selectErrors.setInt(1, partEmail);

                    try (ResultSet errorsResult = selectErrors.executeQuery();) {
                        List<String> cols = new ArrayList<>(
                                JdbcMetadata.getColumns(errorsResult));
                        int count = 0;
                        StringBuilder rowDumpEmail = new StringBuilder(String.format(
                                "%n%nВ %s превышен лимит в 0 ошибок!%n%n",
                                data.getRunner().getSource().getPoolId()));
                        while (errorsResult.next()) {
                            count++;
                            // при необходимости пишем ошибку в лог
                            String rowDump = String.format(
                                    "Ошибка %s из %s %n[ tableName = REP2_ERRORS_LOG [ row = %s ] ]%s",
                                    count, rowCount,
                                    Jdbc.resultSetToString(errorsResult, cols),
                                    "\n==========================================\n");
                            rowDumpEmail.append(rowDump);
                        }
                        rowDumpEmail.append("Всего ");
                        rowDumpEmail.append(rowCount);
                        rowDumpEmail.append(
                                " ошибочных записей. Полный список ошибок доступен в таблице REP2_ERRORS_LOG.");
                        LOG.error(rowDumpEmail.toString());
                    }
                }
            }
        }
    }
}