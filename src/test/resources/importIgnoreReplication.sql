--Settings


--Connection source
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('source', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 1, 100, 1, 10000, 0);
--Connection dest
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('dest', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/dest', 'sa', '', 1, 100, 1, 10000, 0);
--Connection stats
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('stats', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/stats', 'sa', '', 1, 100, 1, 10000, 0);
--Connection error
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('error', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/error', 'sa', '', 1, 100, 1, 10000, 0);

--application_settings
insert into application_settings (key, value) values ('tp.threads', '10');
insert into application_settings (key, value) values ('stats.dest', 'source');
insert into application_settings (key, value) values ('error.dest', 'source');


--Tables
insert into tables (id_table, id_pool, name) values (1, 'source', 'T_TABLE');
insert into tables (id_table, id_pool, name) values (2, 'source', 'T_TABLE1');


--Runners Super Log
insert into runners (id_runner, source, target, description) values (1, 'source', 'source', 'description');
--Strategies Add Super Log
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (1, 'ru.taximaxim.dbreplicator2.replica.strategies.superlog.Manager', 'key1=value1
key2=''value2''', true, 100, 1);

--Runners Task

--Runner Table 1
insert into runners (id_runner, source, target, description) values (3, 'source', 'dest', 'description');
--Strategy  Table 1
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (3, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', null, true, 100, 3);

-------

--Runner Table 2
insert into runners (id_runner, source, target, description) values (4, 'source', 'dest', 'description');
--Strategy  Table 2
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (4, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', null, true, 100, 4);


--Runner tables
insert into table_observers (id_runner, id_table) values (3, 1);
insert into table_observers (id_runner, id_table) values (4, 2);


--Runner CountWatchgdog
insert into runners (id_runner, source, target, description) values (7, 'source', 'source', 'ErrorsCountWatchgdogStrategy');
--Strategy  CountWatchgdog
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (7, 'ru.taximaxim.dbreplicator2.replica.strategies.errors.CountWatchgdog', 'maxErrors=0
partEmail=10', true, 100, 7);
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (10, 'ru.taximaxim.dbreplicator2.replica.strategies.errors.CountWatchgdog', null, true, 100, 7);


--Ignore Columns Table
insert into ignore_columns_table (id_ignore_columns_table, id_table, column_name) values (1, 1, '_STRING');
insert into ignore_columns_table (id_ignore_columns_table, id_table, column_name) values (2, 2, '_STRING');

-- Required Columns Table
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (1, 2, 'ID');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (2, 2, '_INT');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (3, 2, '_BOOLEAN');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (4, 2, '_DATE');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (5, 2, '_TIME');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (6, 2, '_TIMESTAMP');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (7, 2, '_NOCOLOMN');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (8, 2, '_STRING');

insert into required_columns_table (id_required_columns_table, id_table, column_name) values ( 9, 1, 'ID');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (10, 1, '_INT');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (11, 1, '_BOOLEAN');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (12, 1, '_DATE');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (13, 1, '_TIME');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (14, 1, '_TIMESTAMP');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (15, 1, '_NOCOLOMN');
insert into required_columns_table (id_required_columns_table, id_table, column_name) values (16, 1, '_STRING');
