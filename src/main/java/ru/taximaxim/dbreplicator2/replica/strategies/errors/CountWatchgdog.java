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
import java.util.List;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.jdbc.Jdbc;
import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;
import ru.taximaxim.dbreplicator2.model.StrategyModel;
import ru.taximaxim.dbreplicator2.replica.Strategy;
import ru.taximaxim.dbreplicator2.replica.StrategyException;

/**
 * Класс стратегии репликации данных из источника в приемник
 * Записи реплицируются в порядке последних операций над ними.
 * 
 * @author volodin_aa
 * 
 */
public class CountWatchgdog implements Strategy {

    private static final Logger LOG = Logger.getLogger(CountWatchgdog.class);

    /**
     * Конструктор по умолчанию
     */
    public CountWatchgdog() {
    }

    @Override
    public void execute(Connection sourceConnection, Connection targetConnection,
            StrategyModel data) throws StrategyException, SQLException {
        
        int maxErrors = 0;
        if(data.getParam("maxErrors")!=null) {
            maxErrors = Integer.parseInt(data.getParam("maxErrors"));
        }
        
        int partEmail = 10;
        if(data.getParam("partEmail")!=null) {
            partEmail = Integer.parseInt(data.getParam("partEmail"));
        }
        
        // Проверияем количество ошибочных итераций
        try (PreparedStatement selectErrors = 
                sourceConnection.prepareStatement(
                        "SELECT *, " +
                        "(SELECT count(*) FROM rep2_workpool_data where c_errors_count>?) as count" +
                        " FROM rep2_workpool_data WHERE id_superlog IN " +
                        "(SELECT MAX(id_superlog) FROM rep2_workpool_data AS last_data WHERE " +
                        "c_errors_count>? GROUP BY id_runner, id_table, id_foreign)" +
                        " ORDER BY c_errors_count desc");
                ) {
            selectErrors.setInt(1, maxErrors);
            selectErrors.setInt(2, maxErrors);
            try (ResultSet errorsResult = selectErrors.executeQuery();) {
                List<String> cols =  JdbcMetadata.getColumnsList(errorsResult);
                Integer rowCount = null;
                int count = 0;
                StringBuffer rowDumpEmail = new StringBuffer();
                while (errorsResult.next()) {
                    if(rowCount==null) {
                        rowCount = errorsResult.getInt("count");
                    }
                    count++;
                    // при необходимости пишем ошибку в лог
                    String rowDump = String.format(
                            "Ошибок: (%s/%s)\nПревышен лимит в %s ошибок!\n" +
                            "[ tableName = REP2_WORKPOOL_DATA [ row = %s ] ]%s",
                            count,
                            rowCount,
                            maxErrors,
                            Jdbc.resultSetToString(errorsResult, cols),
                            "\n==========================================\n"
                            );
                    rowDumpEmail.append(rowDump);
                    if(count % partEmail == 0) {
                        LOG.error(rowDumpEmail.toString()+
                          "Всего "+rowCount+
                         " ошибочных записей. Полный список ошибок доступен в таблице rep2_workpool_data");
                        rowDumpEmail = new StringBuffer();
                    }
                } 
                if(count % partEmail != 0) {
                    LOG.error(rowDumpEmail.toString()+
                         "Всего "+rowCount+
                         " ошибочных записей. Полный список ошибок доступен в таблице rep2_workpool_data");
                }
            }
        }
    }

}