--Settings
--application_settings
insert into application_settings (key, value) values ('tp.threads', '10');
insert into application_settings (key, value) values ('stats.dest', 'source');
insert into application_settings (key, value) values ('error.dest', 'source');

--Connection source
insert into hikari_cp_settings (id_pool, driver, url, user, pass, max_pool_size, init_fail_fast, connection_timeout, idle_timeout, max_lifetime) values ('source', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 10, false, 10000, 10000, 10000);
--Connection dest
insert into hikari_cp_settings (id_pool, driver, url, user, pass, max_pool_size, init_fail_fast, connection_timeout, idle_timeout, max_lifetime) values ('dest', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/dest' , 'sa', '', 10, false, 10000, 10000, 10000);

--Runners Super Log
insert into runners (id_runner, source, target, description) values (1, 'source', 'source', 'SuperlogRunner');
--Strategies Add Super Log
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (1, 'ru.taximaxim.dbreplicator2.replica.strategies.superlog.FastManager', null, true, 100, 1);

-------

--Runner Table 2
insert into runners (id_runner, source, target, description) values (2, 'source', 'dest', 'ReplicaRunner');
--Strategy  Table 2
insert into strategies (id_runner, id, className, param, isEnabled, priority) 
values (2, 2, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', 'fetchSize=1
batchSize=1', true, 100);
insert into tables (id_runner, name, param) 
values (2, 'T_TABLE2', 'ignoredCols=_STRING');

--Runner Table 3
insert into runners (id_runner, source, target, description) values (3, 'source', 'dest', 'ReplicaRunner');
--Strategy  Table 3
insert into strategies (id_runner, id, className, param, isEnabled, priority) 
values (3, 3, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', 'fetchSize=1
batchSize=1', true, 100);
insert into tables (id_runner, name, param) 
values (3, 'T_TABLE3', 'ignoredCols=_STRING');

-------

--Runner CountWatchgdog
insert into runners (id_runner, source, target, description) values (7, 'source', 'source', 'ErrorsCountWatchgdogStrategy');
--Strategy  CountWatchgdog
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (7, 'ru.taximaxim.dbreplicator2.replica.strategies.errors.CountWatchgdog', 'maxErrors=0
partEmail=10', true, 100, 7);

-- Cron
insert into cron (id_task, id_runner, enabled, cron_string, description) values (1, 1, true, null, 'test');

insert into cron (id_task, id_runner, enabled, cron_string, description) values (2, 3, true, '*/1 * * * * ?', 'test');

