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

import ru.taximaxim.dbreplicator2.jdbc.Jdbc;
import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;

/**
 * @author mardanov_rm
 * 
 */
public class ReplicationTimeWatchgdog implements Strategy {

    private static final Logger LOG = Logger.getLogger(ReplicationTimeWatchgdog.class);

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
     * @param s
     * @return
     */
    public String removeLastChar(String s) {    
        if (s == null || s.length() == 0) {        
            return s;    
        }   
        return s.substring(0, s.length()-1); 
     }   
    
    @Override
    public void execute(Connection sourceConnection, Connection targetConnection,
            StrategyModel data) throws StrategyException, SQLException,
            ClassNotFoundException {
        try {
            int period = 1800000;
            if (data.getParam(PERIOD) != null) {
                period = Integer.parseInt(data.getParam(PERIOD));
            }
    
            Timestamp date = new Timestamp(Calendar.getInstance().getTimeInMillis() - period);
    
            int partEmail = 10;
            if (data.getParam(PART_EMAIL) != null) {
                partEmail = Integer.parseInt(data.getParam(PART_EMAIL));
            }
            
            String runIgSql = "";
            if (data.getParam(RUNNERS) != null) {
                String runnerIgnore = "";
                String runnerAktiv = "";
                StringTokenizer totoken = new StringTokenizer(data.getParam(RUNNERS).toString(), ",");
                while (totoken.hasMoreTokens()) {
                    String str = totoken.nextToken();
                    if(Integer.parseInt(str) < 0) {
                        runnerIgnore += str + ",";
                    } else {
                        runnerAktiv += str + ",";
                    }
                }
                if(runnerIgnore != "") {
                    runnerIgnore.substring(0, runnerIgnore.length()-1);
                    runIgSql += String.format(" AND id_runner NOT IN (%s)", 
                            removeLastChar(runnerIgnore).replace("-", ""));
                }
                if(runnerAktiv != ""){
                    runnerAktiv.substring(runnerAktiv.length()-1);
                    runIgSql += String.format(" AND id_runner IN (%s)", 
                            removeLastChar(runnerAktiv));
                }
               
            }
            
            int rowCount = 0;
            try (PreparedStatement selectErrorsCount = sourceConnection
                  .prepareStatement(
                    "SELECT count(*) as count FROM rep2_workpool_data WHERE c_date <= ?"
                          + runIgSql);) {
    
                selectErrorsCount.setTimestamp(1, date);
                try (ResultSet countResult = selectErrorsCount.executeQuery();) {
                    if (countResult.next()) {
                        rowCount = countResult.getInt(COUNT);
                    }
                }
            }
            // Если нет ошибок то смысл в запуске данного кода бессмыслен
            if (rowCount != 0) {
                try (PreparedStatement selectPreparedStatement = sourceConnection
                      .prepareStatement(
                        "SELECT * FROM rep2_workpool_data WHERE c_date <= ? " + runIgSql+ " ORDER BY id_superlog",
                                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);) {
    
                    selectPreparedStatement.setTimestamp(1, date);
                    selectPreparedStatement.setFetchSize(partEmail);
                    try (ResultSet resultSet = selectPreparedStatement.executeQuery();) {
                        List<String> cols = new ArrayList<String>(
                                JdbcMetadata.getColumns(resultSet));
                        int count = 0;
                        StringBuffer rowDumpEmail = new StringBuffer(
                                String.format(
                                        "\n\nВ %s превышен лимит таймаута репликации в %s милисекунд!\n\n",
                                        data.getRunner().getSource().getPoolId(), period));
                        while (resultSet.next() && (count < partEmail)) {
                            count++;
                            // при необходимости пишем ошибку в лог
                            String rowDump = String
                                    .format("Запись не реплицируется!!!\nОшибка %s из %s \n[ tableName = REP2_WORKPOOL_DATA [ row = %s ] ]%s",
                                            count, rowCount,
                                            Jdbc.resultSetToString(resultSet, cols),
                                            "\n==========================================\n");
                            rowDumpEmail.append(rowDump);
                        }
                        rowDumpEmail.append("Всего ");
                        rowDumpEmail.append(rowCount);
                        rowDumpEmail
                                .append(" ошибочных записей. Полный список ошибок не реплицируеммых записей доступен в таблице rep2_workpool_data.");
                        LOG.error(rowDumpEmail.toString());
                    }
                }
            }
        } catch (NumberFormatException e) {
            LOG.error(String.format(
               "Ошибка в преобразование строки в число стратегия: ReplicationTimeWatchgdog: [%s] runner:[%s]",
               data.getId(),data.getRunner().getId()),e);
        }
    }
}
