--Settings

--Connection
insert into bone_cp_settings (pool_id, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('source', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 1, 100, 1, 10000, 0)

--Runners Super Log
insert into runners (id, source, target, description, class_name) values (1, 'source', 'source', 'description', 'ru.taximaxim.dbreplicator2.replica.SuperlogRunner')

--Strategies Add Super Log
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (1, 'ru.taximaxim.dbreplicator2.replica.strategies.SuperLogManagerStrategy', null, true, 100, 1)

--Runners Task

--Runners Dummy Strategy
insert into runners (id, source, target, description, class_name) values (2, 'source', 'dest', 'description', 'ru.taximaxim.dbreplicator2.replica.ReplicaRunner')

--Strategies Dummy Strategy
insert into strategies (id, className, param, isEnabled, priority, id_runner) values (2, 'ru.taximaxim.dbreplicator2.replica.strategies.ReplicationStrategy', null, true, 100, 2)

--Connection
insert into bone_cp_settings (pool_id, driver, url, user, pass, min_connections_per_partition, max_connections_per_partition, partition_count, connection_timeout_in_ms, close_connection_watch_timeout_in_ms ) values ('dest', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/dest', 'sa', '', 1, 100, 1, 10000, 0)
