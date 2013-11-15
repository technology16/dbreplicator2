drop table if exists rep2_superlog;
drop table if exists rep2_workpool_data;
CREATE TABLE rep2_superlog(id_superlog IDENTITY PRIMARY KEY, id_foreign INTEGER, id_table NVARCHAR, c_operation VARCHAR(1), c_date TIMESTAMP, id_transaction NVARCHAR);
CREATE TABLE rep2_workpool_data(id_runner INTEGER, id_superlog BIGINT, id_foreign INTEGER, id_table NVARCHAR, c_operation VARCHAR(1), c_date TIMESTAMP, id_transaction NVARCHAR, c_errors_count INTEGER, c_last_error NVARCHAR, c_last_error_date TIMESTAMP);
