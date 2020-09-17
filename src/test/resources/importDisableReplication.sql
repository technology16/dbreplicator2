--Connections
insert into hikari_cp_settings 
(id_pool, driver, url, user, pass, max_pool_size, init_fail_fast, connection_timeout, idle_timeout, max_lifetime, param) values 
('broken',           'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 10, false, 10000, 10000, 10000, 'is_enabled=broken'),
('source',           'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 10, false, 10000, 10000, 10000, ''),
('source_enabled',   'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 10, false, 10000, 10000, 10000, 'is_enabled=true'),
('source_disabled',  'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 10, false, 10000, 10000, 10000, 'is_enabled=false'),
('dest',             'org.h2.Driver', 'jdbc:h2:mem://localhost/~/dest',   'sa', '', 10, false, 10000, 10000, 10000, ''),
('dest_enabled',     'org.h2.Driver', 'jdbc:h2:mem://localhost/~/dest',   'sa', '', 10, false, 10000, 10000, 10000, 'is_enabled=true'),
('dest_disabled',    'org.h2.Driver', 'jdbc:h2:mem://localhost/~/dest',   'sa', '', 10, false, 10000, 10000, 10000, 'is_enabled=false'),
('stats',            'org.h2.Driver', 'jdbc:h2:mem://localhost/~/stats',  'sa', '', 10, false, 10000, 10000, 10000, ''),
('error',            'org.h2.Driver', 'jdbc:h2:mem://localhost/~/error',  'sa', '', 10, false, 10000, 10000, 10000, '');

--Settings
insert into application_settings (key, value) values 
('tp.threads', '10'),
('stats.dest', 'source'),
('error.dest', 'source');

--Runners
insert into runners (id_runner, source, target, description) values 
(1,  'source',          'source',          'superlog'),
(2,  'source_enabled',  'source_enabled',  'superlog_enabled'),
(3,  'source_disabled', 'source_disabled', 'superlog_disabled'),
(10, 'source',          'dest_enabled',    'standard to enabled'),
(11, 'source',          'dest_disabled',   'standard to disabled'),
(12, 'source_enabled',  'dest',            'enabled to standard'),
(13, 'source_enabled',  'dest_enabled',    'enabled to enabled'),
(14, 'source_enabled',  'dest_disabled',   'enabled to disabled'),
(15, 'source_disabled', 'dest',            'disabled to standard'),
(16, 'source_disabled', 'dest_enabled',    'disabled to enabled'),
(17, 'source_disabled', 'dest_disabled',   'disabled to disabled'),
(18, 'broken',          'dest',            'broken to standard');

--Strategies
insert into strategies (id, id_runner, className, param, isEnabled, priority) values
(1,  1,  'ru.taximaxim.dbreplicator2.replica.strategies.superlog.Manager',     null, true, 100),
(2,  2,  'ru.taximaxim.dbreplicator2.replica.strategies.superlog.Manager',     null, true, 100),
(3,  3,  'ru.taximaxim.dbreplicator2.replica.strategies.superlog.Manager',     null, true, 100),
(10, 10, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic',  null, true, 100),
(11, 11, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic',  null, true, 100),
(12, 12, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic',  null, true, 100),
(13, 13, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic',  null, true, 100),
(14, 14, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic',  null, true, 100),
(15, 15, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic',  null, true, 100),
(16, 16, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic',  null, true, 100),
(17, 17, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic',  null, true, 100),
(18, 18, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic',  null, true, 100);

--Tables
insert into tables (name, id_runner, param) values 
('T_TABLE',  10, 'ignoredCols=_STRING'),
('T_TABLE1', 11, 'ignoredCols=_STRING'),
('T_TABLE2', 12, 'ignoredCols=_STRING'),
('T_TABLE3', 13, 'ignoredCols=_STRING'),
('T_TABLE4', 14, 'ignoredCols=_STRING'),
('T_TABLE5', 15, 'ignoredCols=_STRING'),
('T_TABLE6', 16, 'ignoredCols=_STRING'),
('T_TABLE7', 17, 'ignoredCols=_STRING'),
('T_TABLE8', 18, 'ignoredCols=_STRING');