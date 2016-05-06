drop table if exists rep2_superlog;
drop table if exists rep2_workpool_data;
CREATE TABLE rep2_superlog(id_superlog IDENTITY PRIMARY KEY, id_foreign BIGINT, id_table NVARCHAR, c_operation VARCHAR(1), c_date TIMESTAMP, id_transaction NVARCHAR, id_pool  NVARCHAR);
CREATE TABLE rep2_workpool_data(id_runner INTEGER NOT NULL, id_superlog BIGINT NOT NULL, id_foreign BIGINT, id_table NVARCHAR, c_operation VARCHAR(1), c_date TIMESTAMP, id_transaction NVARCHAR);
ALTER TABLE rep2_workpool_data ADD PRIMARY KEY (id_runner, id_superlog);
drop table if exists rep2_statistics;
CREATE TABLE rep2_statistics(id_statistics IDENTITY PRIMARY KEY,c_date TIMESTAMP,c_type INTEGER, id_strategy INTEGER,id_table NVARCHAR, c_count BIGINT);
drop table if exists rep2_errors_log;
CREATE TABLE rep2_errors_log(id_errors_log IDENTITY PRIMARY KEY, id_runner INTEGER, id_table NVARCHAR, id_foreign BIGINT, c_date TIMESTAMP, c_error NVARCHAR, c_status INTEGER NOT NULL)
