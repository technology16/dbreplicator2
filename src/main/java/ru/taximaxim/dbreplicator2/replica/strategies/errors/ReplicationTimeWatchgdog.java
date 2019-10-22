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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.cf.ConnectionFactory;
import ru.taximaxim.dbreplicator2.el.FatalReplicationException;
import ru.taximaxim.dbreplicator2.jdbc.Jdbc;
import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;

/**
 * @author mardanov_rm
 * 
 */
public class ReplicationTimeWatchgdog implements Strategy {

    private static final Logger LOG = Logger.getLogger(ReplicationTimeWatchgdog.class);

    private static final int DEFAULT_PERIOD = 1800000;
    private static final int DEFAULT_PART_EMAIL = 10;

    private static final String PERIOD = "period";
    private static final String RUNNERS = "runners";
    private static final String PART_EMAIL = "partEmail";
    private static final String COUNT = "count";

    /**
     * Конструктор по умолчанию
     */
    public ReplicationTimeWatchgdog() {
    }

    /**
     * Последний символ
     * 
     * @param s
     * @return
     */
    public String removeLastChar(String s) {
        if (s == null || s.length() == 0) {
            return s;
        }
        return s.substring(0, s.length() - 1);
    }

    @Override
    public void execute(ConnectionFactory connectionsFactory, StrategyModel data)
            throws SQLException, FatalReplicationException {
        StringBuilder runIgSql = new StringBuilder();
        int period = DEFAULT_PERIOD;
        int partEmail = DEFAULT_PART_EMAIL;

        try {
            if (data.getParam(PERIOD) != null) {
                period = Integer.parseInt(data.getParam(PERIOD));
            }

            if (data.getParam(PART_EMAIL) != null) {
                partEmail = Integer.parseInt(data.getParam(PART_EMAIL));
            }

            if (data.getParam(RUNNERS) != null) {
                StringBuilder runnerIgnore = new StringBuilder();
                StringBuilder runnerAktiv = new StringBuilder();
                StringTokenizer totoken = new StringTokenizer(
                        data.getParam(RUNNERS), ",");
                while (totoken.hasMoreTokens()) {
                    String str = totoken.nextToken();
                    if (Integer.parseInt(str) < 0) {
                        runnerIgnore.append(str).append(",");
                    } else {
                        runnerAktiv.append(str).append(",");
                    }
                }
                if (runnerIgnore.length() > 0) {
                    runIgSql.append(String.format(" AND id_runner NOT IN (%s)",
                            removeLastChar(runnerIgnore.toString()).replace("-", "")));
                }
                if (runnerAktiv.length() > 0) {
                    runIgSql.append(String.format(" AND id_runner IN (%s)",
                            removeLastChar(runnerAktiv.toString())));
                }
            }
        } catch (NumberFormatException e) {
            LOG.error(String.format(
                    "Ошибка в преобразование строки в число стратегия: ReplicationTimeWatchgdog: [%s] runner:[%s]",
                    data.getId(), data.getRunner().getId()), e);
        }

        int rowCount = 0;
        String selectErrorsCountQuery = "SELECT count(*) as count FROM rep2_workpool_data WHERE c_date <= ?"
                + runIgSql;
        Timestamp date = new Timestamp(Calendar.getInstance().getTimeInMillis() - period);
        try (Connection sourceConnection = connectionsFactory
                .get(data.getRunner().getSource().getPoolId()).getConnection();) {
            try (PreparedStatement selectErrorsCount = sourceConnection
                    .prepareStatement(selectErrorsCountQuery);) {

                selectErrorsCount.setTimestamp(1, date);
                try (ResultSet countResult = selectErrorsCount.executeQuery();) {
                    if (countResult.next()) {
                        rowCount = countResult.getInt(COUNT);
                    }
                }
            }
            // Если нет ошибок то смысл в запуске данного кода бессмыслен
            if (rowCount != 0) {
                String selectQuery = "SELECT * FROM rep2_workpool_data WHERE c_date <= ? "
                        + runIgSql + " ORDER BY id_superlog";
                try (PreparedStatement selectPreparedStatement = sourceConnection
                        .prepareStatement(selectQuery, ResultSet.TYPE_FORWARD_ONLY,
                                ResultSet.CONCUR_READ_ONLY);) {
                    selectPreparedStatement.setTimestamp(1, date);
                    selectPreparedStatement.setFetchSize(partEmail);
                    try (ResultSet resultSet = selectPreparedStatement.executeQuery();) {
                        List<String> cols = new ArrayList<>(
                                JdbcMetadata.getColumns(resultSet));
                        int count = 0;
                        StringBuilder rowDumpEmail = new StringBuilder(String.format(
                                "%n%nВ %s превышен лимит таймаута репликации в %s милисекунд!%n%n",
                                data.getRunner().getSource().getPoolId(), period));
                        while (resultSet.next() && (count < partEmail)) {
                            count++;
                            // при необходимости пишем ошибку в лог
                            String rowDump = String.format(
                                    "Запись не реплицируется!!!%nОшибка %s из %s %n[ tableName = REP2_WORKPOOL_DATA [ row = %s ] ]%s",
                                    count, rowCount,
                                    Jdbc.resultSetToString(resultSet, cols),
                                    "\n==========================================\n");
                            rowDumpEmail.append(rowDump);
                        }
                        rowDumpEmail.append("Всего ");
                        rowDumpEmail.append(rowCount);
                        rowDumpEmail.append(
                                " ошибочных записей. Полный список ошибок не реплицируеммых записей доступен в таблице rep2_workpool_data.");
                        LOG.error(rowDumpEmail.toString());
                    }
                }
            }
        }
    }
}
