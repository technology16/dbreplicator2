--Settings
--application_settings
insert into application_settings (key, value) values ('tp.threads', '5');
insert into application_settings (key, value) values ('stats.dest', 'source');
insert into application_settings (key, value) values ('error.dest', 'source');

--Connection source
insert into hikari_cp_settings (id_pool, driver, url, user, pass, max_pool_size, init_fail_fast, connection_timeout, idle_timeout, max_lifetime) 
values ('source', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 5, false, 10000, 10000, 10000);
--Connection dest
insert into hikari_cp_settings (id_pool, driver, url, user, pass, max_pool_size, init_fail_fast, connection_timeout, idle_timeout, max_lifetime) 
values ('dest', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/dest'  , 'sa', '', 5, false, 10000, 10000, 10000);

--Runners Super Log
insert into runners (id_runner, source, target, description) values (1, 'source', 'source', 'SuperlogRunner');
--Strategies Add Super Log
-- Выбираем по 3 записи, при этом в тесте 2 запись будет ошибочная
insert into strategies (id, className, param, isEnabled, priority, id_runner) 
values (1, 'ru.taximaxim.dbreplicator2.replica.strategies.superlog.FastManager', 'fetchSize=3', true, 100, 1);

-------

--Runner 2
insert into runners (id_runner, source, target, description) values (2, 'source', 'dest', 'ReplicaRunner');
--Strategy  Table 1,2
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (2, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', 'fetchSize=1', true, 100, 2);
insert into tables (name, id_runner, param) values ('T_TABLE2', 2, 'ignoredCols=_STRING');
insert into tables (name, id_runner, param) values ('T_TABLE3', 2, 'ignoredCols=_STRING');
-------

--Runner 3 с отключеной стратегией
insert into runners (id_runner, source, target, description) 
values (3, 'source', 'dest', 'ReplicaRunner');
insert into tables (id_runner, name, param) 
values (3, 'T_TABLE4', 'ignoredCols=_STRING');
insert into tables (id_runner, name, param) 
values (3, 'T_TABLE5', 'ignoredCols=_STRING');
insert into tables (id_runner, name, param) 
values (3, 'T_TABLE6', 'ignoredCols=_STRING');
