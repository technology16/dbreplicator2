--Settings

--Connection
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('source', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 1, 100, 1, 10000, 0)

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
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (1, 'ru.taximaxim.dbreplicator2.replica.strategies.FastSuperLogManagerStrategy', null, true, 100, 1)

--Runner
insert into runners (id_runner, source, target, description, class_name) values (2, 'source', 'dest', 'description', 'ru.taximaxim.dbreplicator2.replica.ReplicaRunner')

--Runner tables
insert into table_observers (id_runner, id_table) values (2, 1)
insert into table_observers (id_runner, id_table) values (2, 2)
insert into table_observers (id_runner, id_table) values (2, 3)
insert into table_observers (id_runner, id_table) values (2, 4)
insert into table_observers (id_runner, id_table) values (2, 5)
insert into table_observers (id_runner, id_table) values (2, 6)

--Strategy
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (2, 'ru.taximaxim.dbreplicator2.replica.strategies.ReplicationStrategy', null, true, 100, 2)

--Connection
insert into bone_cp_settings (id_pool, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('dest', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/dest', 'sa', '', 1, 100, 1, 10000, 0)
