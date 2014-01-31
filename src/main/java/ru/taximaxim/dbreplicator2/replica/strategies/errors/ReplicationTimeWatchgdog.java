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
    private static final String PART_EMAIL = "partEmail";
    private static final String COUNT = "count";
    /**
     * Конструктор по умолчанию
     */
    public ReplicationTimeWatchgdog() {
    }
    
    @Override
    public void execute(Connection sourceConnection, Connection targetConnection,
            StrategyModel data) throws StrategyException, SQLException,
            ClassNotFoundException {
        
        int period = 1800000;
        if(data.getParam(PERIOD)!=null) {
            period = Integer.parseInt(data.getParam(PERIOD));
        }

        Timestamp date = new Timestamp(Calendar.getInstance().getTimeInMillis() - period);
        
        int partEmail = 10;
        if(data.getParam(PART_EMAIL)!=null) {
            partEmail = Integer.parseInt(data.getParam(PART_EMAIL));
        }
        
        try (PreparedStatement selectPreparedStatement = sourceConnection.prepareStatement(
                "SELECT * FROM rep2_workpool_data WHERE id_superlog IN " +
                "(SELECT MAX(id_superlog) AS id_superlog FROM rep2_workpool_data WHERE " +
                " c_date <= ? and c_errors_count = 0 " +
                "GROUP BY id_foreign, id_table ORDER BY id_superlog) ORDER BY id_superlog");
                
                PreparedStatement selectErrorsCount = 
                        sourceConnection.prepareStatement(
                "SELECT count(*) as count FROM rep2_workpool_data WHERE id_superlog IN " +
                "(SELECT MAX(id_superlog) AS id_superlog FROM rep2_workpool_data WHERE " +
                " c_date <= ? and c_errors_count = 0 " +
                "GROUP BY id_foreign, id_table)");) {

            int rowCount = 0;
            selectErrorsCount.setTimestamp(1, date);
            try (ResultSet countResult = selectErrorsCount.executeQuery();) {
                while (countResult.next()) {
                    rowCount = countResult.getInt(COUNT);
                }
            }
            //Если нет ошибок то смысл в запуске данного кода бессмыслен 
            if(rowCount != 0) {
                selectPreparedStatement.setTimestamp(1, date);
                
                try (ResultSet resultSet = selectPreparedStatement.executeQuery();) {
                    List<String> cols = new ArrayList<String>(JdbcMetadata.getColumns(resultSet));
                    int count = 0;
                    StringBuffer rowDumpEmail = new StringBuffer(String.format(
                        "\n\nВ %s превышен лимит таймаута репликации в %s милисекунд!\n\n",
                        data.getRunner().getSource().getPoolId(), period));
                    while (resultSet.next() && (count < partEmail)) {
                        count++;
                        // при необходимости пишем ошибку в лог
                        String rowDump = String.format(
                                "Запись не реплицируется!!!\nОшибка %s из %s \n[ tableName = REP2_WORKPOOL_DATA [ row = %s ] ]%s",
                                count,
                                rowCount,
                                Jdbc.resultSetToString(resultSet, cols),
                                "\n==========================================\n"
                                );
                        rowDumpEmail.append(rowDump);
                    } 
                    rowDumpEmail.append("Всего ");
                    rowDumpEmail.append(rowCount);
                    rowDumpEmail.append(" ошибочных записей. Полный список ошибок не реплицируеммых записей доступен в таблице rep2_workpool_data.");
                    LOG.error(rowDumpEmail.toString());
                }
            }
        }
    }

}
