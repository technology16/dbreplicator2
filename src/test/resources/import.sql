--Settings


--Connection source
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('source', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 1, 100, 1, 10000, 0)
--Connection dest
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('dest', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/dest', 'sa', '', 1, 100, 1, 10000, 0)


--application_settings
insert into application_settings (key, value) values ('tp.threads', '10')


--Tables
insert into tables (id_table, id_pool, name) values (1, 'source', 't_table')
insert into tables (id_table, id_pool, name) values (2, 'source', 't_table1')
insert into tables (id_table, id_pool, name) values (3, 'source', 't_table2')
insert into tables (id_table, id_pool, name) values (4, 'source', 't_table3')
insert into tables (id_table, id_pool, name) values (5, 'source', 't_table4')
insert into tables (id_table, id_pool, name) values (6, 'source', 't_table5')


--Runners Super Log
insert into runners (id_runner, source, target, description, class_name) values (1, 'source', 'source', 'description', 'ru.taximaxim.dbreplicator2.replica.SuperlogRunner')
--Strategies Add Super Log
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (1, 'ru.taximaxim.dbreplicator2.replica.strategies.superlog.FastManager', null, true, 100, 1)


--Runners Task

--Runner Table 1
insert into runners (id_runner, source, target, description, class_name) values (2, 'source', 'dest', 'description', 'ru.taximaxim.dbreplicator2.replica.ReplicaRunner')
--Strategy  Table 1
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (2, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', null, true, 100, 2)

-------

--Runner Table 2
insert into runners (id_runner, source, target, description, class_name) values (3, 'source', 'dest', 'description', 'ru.taximaxim.dbreplicator2.replica.ReplicaRunner')
--Strategy  Table 2
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (3, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', null, true, 100, 3)

-------

--Runner Table 3
insert into runners (id_runner, source, target, description, class_name) values (4, 'source', 'dest', 'description', 'ru.taximaxim.dbreplicator2.replica.ReplicaRunner')
--Strategy  Table 3
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (4, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', null, true, 100, 4)

-------

--Runner Table 4,5,6
insert into runners (id_runner, source, target, description, class_name) values (5, 'source', 'dest', 'description', 'ru.taximaxim.dbreplicator2.replica.ReplicaRunner')
--Strategy  Table 4,5,6
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (5, 'ru.taximaxim.dbreplicator2.replica.strategies.replication.Generic', null, true, 100, 5)

-------


--Runner tables
insert into table_observers (id_runner, id_table) values (2, 1)
insert into table_observers (id_runner, id_table) values (3, 2)
insert into table_observers (id_runner, id_table) values (4, 6)
insert into table_observers (id_runner, id_table) values (5, 3)
insert into table_observers (id_runner, id_table) values (5, 4)
insert into table_observers (id_runner, id_table) values (5, 5)

--Runner CountWatchgdog
insert into runners (id_runner, source, target, description, class_name) values (6, 'source', 'source', 'ErrorsCountWatchgdogStrategy', '')
--Strategy  CountWatchgdog
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (6, 'ru.taximaxim.dbreplicator2.replica.strategies.errors.CountWatchgdog', null, true, 100, 6)

--Ignore Columns Table
insert into ignore_columns_table (id_ignore_columns_table, id_table, column_name) values (1, 2, '_decimal')
insert into ignore_columns_table (id_ignore_columns_table, id_table, column_name) values (2, 2, '_int')