package ru.taximaxim.dbreplicator2.replica.strategies.errors;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

import org.apache.log4j.Logger;


public class ErrorsLog implements ErrorsLogService, AutoCloseable{
    
    private static final Logger LOG = Logger.getLogger(ErrorsLog.class);
    /**
     * кусок sql запрос обновление статуса
     */
    private static final String SQL_UPDATE = "UPDATE rep2_errors_log SET c_status = ? where ";
    /**
     * подготовленый запрос выборки 
     */
    private PreparedStatement selectStatement;
    /**
     * подготовленый запрос вставки
     */
    private PreparedStatement insertStatement;
    /**
     * подготовленый запрос обноление где id_runner is null
     */
    private PreparedStatement updateStatement;
    /**
     * подготовленый запрос обноление где id_runner is null
     */
    private PreparedStatement updateStatementRunner;
    /**
     * подготовленый запрос обноление где id_table is null
     */
    private PreparedStatement updateStatementTable;
    /**
     * подготовленый запрос обноление где id_foreign is null
     */
    private PreparedStatement updateStatementForeign;
    /**
     * подготовленый запрос обноление где id_runner is null and id_table
     */
    private PreparedStatement updateStatementRunnerTable;
    /**
     * подготовленый запрос обноление где id_runner is null and id_foreign is null
     */
    private PreparedStatement updateStatementRunnerForeign;
    /**
     * подготовленый запрос обноление где id_table is null and id_foreign is null
     */
    private PreparedStatement updateStatementTableForeign;
    /**
     * подготовленый запрос обноление где id_runner is null and id_table is null and id_foreign is null
     */
    private PreparedStatement updateStatementRunnerTableForeign;
    /**
     * Подключение
     */
    private Connection connection;
    
    /**
     * Конструктор на основе соединения к БД 
     */
    public ErrorsLog(Connection connection) {
        this.connection = connection;
    }
    
    /**
     * @return the connection
     */
    protected Connection getConnection() {
        return connection;
    }
    
    @Override
    public void add(Integer runnerId, String tableId, Long foreignId, String error) {
        try {
            PreparedStatement statement = getInsertStatement();
            setStatment(statement, runnerId, tableId, foreignId, 1);
            statement.setTimestamp(4, new Timestamp(new Date().getTime()));
            statement.setString(5, error);
            statement.execute(); 
        } catch (SQLException e) {
            LOG.warn("Ошибка записи ошибки': ", e);
        }       
    }
    
    @Override
    public void setStatus(Integer runnerId, String tableId, Long foreignId, Integer status) {
        try {
            PreparedStatement statement = getUpdateStatement(runnerId, tableId, foreignId);
            statement.setInt(1, status);
            setStatment(statement, runnerId, tableId, foreignId, 2);
            statement.execute();
        } catch (SQLException e) {
            LOG.warn("Ошибка исправления ошибки': ", e);
        }     
    }
    
    /**
     * Установка параметров PreparedStatement c null значениям
     * @param statement
     * @param runnerId
     * @param tableId
     * @param foreignId
     * @param id номер выражения
     * @throws SQLException
     */
    private void setStatment(PreparedStatement statement, Integer runnerId, String tableId, Long foreignId, Integer id) throws SQLException{
        if(isNull(runnerId)) {
            statement.setNull(id++, Types.INTEGER);
        } else {
            statement.setInt(id++, runnerId);
        }
        if(isNull(tableId)) {
            statement.setNull(id++, Types.VARCHAR);
        } else {
            statement.setString(id++, tableId);
        }
        if(isNull(foreignId)) {
            statement.setNull(id++, Types.BIGINT);
        } else {
            statement.setLong(id++, foreignId);
        }
    }
    /**
     * TODO
     * @return
     * @throws SQLException
     */
    public PreparedStatement getSelectStatement() throws SQLException {
        if (selectStatement == null) {
            selectStatement = 
                getConnection().prepareStatement("SELECT * FROM rep2_errors_log where id_runner = ? and id_table = ? and id_foreign = ?",
                        ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY);
        }
        return selectStatement;
    }
    /**
     * получение PreparedStatement sql pfghjc => Вставка
     * @return
     * @throws SQLException
     */
    public PreparedStatement getInsertStatement() throws SQLException {
        if (insertStatement == null) {
            insertStatement = 
                getConnection().prepareStatement("INSERT INTO rep2_errors_log (id_runner, id_table, id_foreign, c_date, c_error, c_status) values (?, ?, ?, ?, ?, 0)");
        }
        return insertStatement;
    }
    /**
     * Проверка на null
     * @param obj
     * @return
     */
    public boolean isNull(Object obj) {
        if(obj == null) {
            return true;
        }
        return false;
    }
    /**
     * Получение контрольной суммы
     * @param runnerId
     * @param tableId
     * @param foreignId
     * @return
     */
    public int getCheckSum (Integer runnerId, String tableId, Long foreignId) {
        int checkSum = 0;
        if(isNull(runnerId)) {
            checkSum = checkSum + 1;
        }
        if(isNull(tableId)) {
            checkSum = checkSum + 10;
        }
        if(isNull(foreignId)) {
            checkSum = checkSum + 100;
        }
        return checkSum;
    }
    /**
     * Формирование sql запроса
     * @param obj
     * @return
     */
    public String getNullSql(Integer obj) {
        StringBuffer sqlQuery = new StringBuffer();
        sqlQuery.append("id_runner");
        if(isNull(obj)) {
            sqlQuery.append(" is ? ");
        } else {
            sqlQuery.append(" = ? ");
        }
        return sqlQuery.toString();
    }
    /**
     * Формирование sql запроса
     * @param obj
     * @return
     */
    public String getNullSql(String obj) {
        StringBuffer sqlQuery = new StringBuffer();
        sqlQuery.append("id_table");
        if(isNull(obj)) {
            sqlQuery.append(" is ? ");
        } else {
            sqlQuery.append(" = ? ");
        }
        return sqlQuery.toString();
    }
    /**
     * Формирование sql запроса
     * @param obj
     * @return
     */
    public String getNullSql(Long obj) {
        StringBuffer sqlQuery = new StringBuffer();
        sqlQuery.append("id_foreign");
        if(isNull(obj)) {
            sqlQuery.append(" is ? ");
        } else {
            sqlQuery.append(" = ? ");
        }
        return sqlQuery.toString();
    }
    /**
     * Формирование sql запроса
     * @param runnerId
     * @param tableId
     * @param foreignId
     * @return
     */
    public String getNullSql(Integer runnerId, String tableId, Long foreignId) {
        StringBuffer sqlQuery = new StringBuffer();
        sqlQuery.append(getNullSql(runnerId));
        sqlQuery.append(" and ");
        sqlQuery.append(getNullSql(tableId));
        sqlQuery.append(" and ");
        sqlQuery.append(getNullSql(foreignId));
        return sqlQuery.toString();
    }
    /**
     * Прлучение PreparedStatement sql-update от контрольной суммы
     * @param runnerId
     * @param tableId
     * @param foreignId
     * @return
     * @throws SQLException
     */
    public PreparedStatement getUpdateStatement(Integer runnerId, String tableId, Long foreignId) throws SQLException {
        switch (getCheckSum(runnerId, tableId, foreignId)) {
        case 0:
            if (updateStatement == null) {
                updateStatement = 
                    getConnection().prepareStatement(SQL_UPDATE + getNullSql(runnerId, tableId, foreignId));
            }
            return updateStatement;
        case 1:
            if (updateStatementRunner == null) {
                updateStatementRunner = 
                    getConnection().prepareStatement(SQL_UPDATE + getNullSql(runnerId, tableId, foreignId));
            }
            return updateStatementRunner;
        case 10:
            if (updateStatementTable == null) {
                updateStatementTable = 
                    getConnection().prepareStatement(SQL_UPDATE + getNullSql(runnerId, tableId, foreignId));
            }
            return updateStatementTable;
        case 11:
            if (updateStatementRunnerTable == null) {
                updateStatementRunnerTable = 
                    getConnection().prepareStatement(SQL_UPDATE + getNullSql(runnerId, tableId, foreignId));
            }
            return updateStatementRunnerTable;
        case 100:
            if (updateStatementForeign == null) {
                updateStatementForeign = 
                    getConnection().prepareStatement(SQL_UPDATE + getNullSql(runnerId, tableId, foreignId));
            }
            return updateStatementForeign;
        case 101:
            if (updateStatementRunnerForeign == null) {
                updateStatementRunnerForeign = 
                    getConnection().prepareStatement(SQL_UPDATE + getNullSql(runnerId, tableId, foreignId));
            }
            return updateStatementRunnerForeign;
        case 110:
            if (updateStatementTableForeign == null) {
                updateStatementTableForeign = 
                    getConnection().prepareStatement(SQL_UPDATE + getNullSql(runnerId, tableId, foreignId));
            }
            return updateStatementTableForeign;
        case 111:
            if (updateStatementRunnerTableForeign == null) {
                updateStatementRunnerTableForeign = 
                    getConnection().prepareStatement(SQL_UPDATE + getNullSql(runnerId, tableId, foreignId));
            }
            return updateStatementRunnerTableForeign;

        default:
            return null;
        }
    }

    @Override
    public void close() throws SQLException {
        close(selectStatement);
        close(updateStatement);
        close(insertStatement);
        close(updateStatementRunner);
        close(updateStatementTable);
        close(updateStatementForeign);
        close(updateStatementRunnerTable);
        close(updateStatementRunnerForeign);
        close(updateStatementTableForeign);
        close(updateStatementRunnerTableForeign);
    }
    
    /**
     * Закрыть PreparedStatement
     * @param statement
     * @throws SQLException
     */
    public void close(PreparedStatement statement) {
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                LOG.warn("Ошибка при попытке закрыть 'statement.close()': ", e);
            }
        }
    } 

}
