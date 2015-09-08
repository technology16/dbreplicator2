--Settings
--application_settings
insert into application_settings (key, value) values ('tp.threads', '10');
insert into application_settings (key, value) values ('stats.dest', 'source');
insert into application_settings (key, value) values ('error.dest', 'source');

--Connection source
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('source', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 1, 100, 1, 10000, 0);
--Connection dest
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('dest', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/dest'  , 'sa', '', 1, 100, 1, 10000, 0);

--Runners Super Log
insert into runners (id_runner, source, target, description) values (1, 'source', 'source', 'QueueManager');
--Strategies Add Super Log
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (1, 'ru.taximaxim.dbreplicator2.replica.strategies.superlog.QueueManager', null, true, 100, 1);

-------

--Runner Table 1,2
insert into runners (id_runner, source, target, description) values (2, 'source', 'dest', 'Generic');
--Strategy  Table 1,2
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (2, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', 'fetchSize=1
batchSize=1', true, 100, 2);

-------

--Runner CountWatchgdog
insert into runners (id_runner, source, target, description) values (7, 'source', 'source', 'ErrorsCountWatchgdogStrategy');
--Strategy  CountWatchgdog
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (7, 'ru.taximaxim.dbreplicator2.replica.strategies.errors.CountWatchgdog', 'maxErrors=0
partEmail=10', true, 100, 7);

--Tables
insert into tables (id_table, name, id_runner, param) values (1, 'T_TABLE2', 2, 'ignoredCols=_STRING');
insert into tables (id_table, name, id_runner, param) values (2, 'T_TABLE3', 2, 'ignoredCols=_STRING');