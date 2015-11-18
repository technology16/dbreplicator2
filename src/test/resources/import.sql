--Settings

--Connection source
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('source', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 1, 100, 1, 10000, 0);
--Connection dest
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('dest', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/dest', 'sa', '', 1, 100, 1, 10000, 0);
--Connection stats
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('stats', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 1, 100, 1, 10000, 0);
--Connection error
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('error', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 1, 100, 1, 10000, 0);

--application_settings
insert into application_settings (key, value) values ('tp.threads', '10');
insert into application_settings (key, value) values ('stats.dest', 'stats');
insert into application_settings (key, value) values ('error.dest', 'error');

--Runner tables

--Runners Super Log
insert into runners (id_runner, source, target, description) values (1, 'source', 'source', 'FastManager');
--Strategies Add Super Log
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (1, 'ru.taximaxim.dbreplicator2.replica.strategies.superlog.FastManager', 'key1=value1
key2=''value2''', true, 100, 1);

--Runners Super Log 2
insert into runners (id_runner, source, target, description) values (2, 'dest', 'dest', 'FastManager');
--Strategies Add Super Log 2
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (2, 'ru.taximaxim.dbreplicator2.replica.strategies.superlog.FastManager', 'key1=value1
key2=''value2''', true, 100, 2);

--Runners Task

--Runner Table 1
insert into runners (id_runner, source, target, description) values (3, 'source', 'dest', 'Generic');
--Strategy  Table 1
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (3, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', null, true, 100, 3);

-------

--Runner Table 2
insert into runners (id_runner, source, target, description) values (4, 'source', 'dest', 'Generic');
--Strategy  Table 2
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (4, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', null, true, 100, 4);

-------

--Runner Table 3
insert into runners (id_runner, source, target, description) values (5, 'source', 'dest', 'Generic');
--Strategy  Table 3
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (5, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', null, true, 100, 5);

-------

--Runner Table 4,5,6
insert into runners (id_runner, source, target, description) values (6, 'source', 'dest', 'Generic');
--Strategy  Table 4,5,6
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (6, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', null, true, 100, 6);

-------

--Runners Task

--Runner Table 8
insert into runners (id_runner, source, target, description) values (8, 'dest', 'source', 'Generic');
--Strategy  Table 8
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (8, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', null, true, 100, 8);

-------

--Runners Task

--Runner Table 9,10,11,12,13
insert into runners (id_runner, source, target, description) values (9, 'dest', 'source', 'Generic');
--Strategy  Table 9,10,11,12,13
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (9, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', null, true, 100, 9);

--Runner CountWatchgdog
insert into runners (id_runner, source, target, description) values (7, 'source', 'source', 'ErrorsCountWatchgdogStrategy');
--Strategy  CountWatchgdog
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (7, 'ru.taximaxim.dbreplicator2.replica.strategies.errors.CountWatchgdog', 'maxErrors=0
partEmail=10', true, 100, 7);
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (10, 'ru.taximaxim.dbreplicator2.replica.strategies.errors.CountWatchgdog', null, true, 100, 7);

--Runner SuperlogWatchgdog
insert into runners (id_runner, source, target, description) values (15, 'source', 'source', 'ErrorsSuperlogWatchgdog');
--Strategy  SuperlogWatchgdog
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (15, 'ru.taximaxim.dbreplicator2.replica.strategies.errors.SuperlogWatchgdog', 'period=1000
partEmail=10', true, 100, 15);

-- Раннер стратегии проверки логирования необработанных исключений в задаче
insert into runners (id_runner, source, target, description) values (16, 'source', 'dest', 'OutOfMemoryErrorStrategy runner');
insert into strategies (id, id_runner, className, param, isEnabled, priority) values (16, 16, 'ru.taximaxim.dbreplicator2.utils.OutOfMemoryErrorStrategy', null, true, 100);

-- Раннер стратегии проверки логирования необработанных исключений в пуле потоков
insert into runners (id_runner, source, target, description) values (17, 'source', 'dest', 'OutOfMemoryErrorStrategy runner');
insert into strategies (id, id_runner, className, param, isEnabled, priority) values (17, 17, 'ru.taximaxim.dbreplicator2.utils.OutOfMemoryErrorStrategy', null, true, 100);

-------
--Runner null
insert into runners (id_runner, source, target, description) values (25, 'source', 'dest', 'Null');
-------
--Runner IntegrityReplicatedData
insert into runners (id_runner, source, target, description) values (18, 'source', 'dest', 'ErrorsIntegrityReplicatedData');
--Strategy  IntegrityReplicatedData
insert into strategies (id, id_runner, className, param, isEnabled, priority) values (18, 18, 'ru.taximaxim.dbreplicator2.replica.strategies.errors.IntegrityReplicatedData', 'period=0
idRunner=25', true, 100);

--Runner ReplicationTimeWatchgdog
insert into runners (id_runner, source, target, description) values (10, 'source', 'source', 'ErrorsReplicationTimeWatchgdog');
--Strategy  ReplicationTimeWatchgdog
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (11, 'ru.taximaxim.dbreplicator2.replica.strategies.errors.ReplicationTimeWatchgdog', 'period=0
partEmail=10', true, 100, 10);

-- Задача с ошибочной стратегией
insert into tasks (id_task, id_runner, enabled, cron_trigger, cron_string, description) values (16, 16, true, false, null, 'ru.taximaxim.dbreplicator2.utils.OutOfMemoryErrorStrategy');

--Tables
insert into tables (name, id_runner, param) values ('T_TABLE', 3, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE1', 25, 'ignoredCols=_STRING
requiredCols=ID,_INT,_BOOLEAN,_LONG,_DECIMAL,_DOUBLE,_FLOAT,_BYTE,_DATE,_TIME,_TIMESTAMP,_NOCOLOMN,_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE2', 25, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE3', 25, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE4', 25, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE5', 25, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE', 8, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE1', 9, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE2', 9, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE3', 9, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE4', 9, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE5', 9, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE6', 25, '');
insert into tables (name, id_runner, param) values ('T_TABLE7', 25, '');
insert into tables (name, id_runner, param) values ('T_TABLE8', 25, '');
insert into tables (name, id_runner, param) values ('T_TABLE1', 4, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE2', 6, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE3', 6, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE4', 6, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE5', 5, 'ignoredCols=_STRING');
