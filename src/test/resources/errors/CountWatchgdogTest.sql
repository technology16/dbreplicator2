--Connection source
insert into hikari_cp_settings
(id_pool, driver, url, user, pass, max_pool_size, init_fail_fast, connection_timeout, idle_timeout, max_lifetime) values 
('source', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 10, false, 10000, 10000, 10000),
('dest', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/dest', 'sa', '', 10, false, 10000, 10000, 10000),
('stats', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 10, false, 10000, 10000, 10000),
('error', 'org.h2.Driver', 'jdbc:h2:mem://localhost/~/source', 'sa', '', 10, false, 10000, 10000, 10000);

--------------------------------------------------------------------------------

--application_settings
insert into application_settings 
(key, value) values 
('tp.threads', '10'),
('stats.dest', 'stats'),
('error.dest', 'error');

--------------------------------------------------------------------------------

insert into runners
(id_runner, source, target, description) values
(1, 'error', 'error', 'Проверка поведения по умолчанию');

insert into strategies
(id_runner, id, className, param, isEnabled, priority) values
(1, 11, 'ru.taximaxim.dbreplicator2.replica.strategies.errors.CountWatchgdog', null, true, 100);

--------------------------------------------------------------------------------

insert into runners
(id_runner, source, target, description) values
(2, 'error', 'error', 'Проверка partEmail=5');

insert into strategies
(id_runner, id, className, param, isEnabled, priority) values
(2, 21, 'ru.taximaxim.dbreplicator2.replica.strategies.errors.CountWatchgdog', 'partEmail=5', true, 100);

--------------------------------------------------------------------------------

insert into runners
(id_runner, source, target, description) values
(3, 'error', 'error', 'Проверка условия where');

insert into strategies
(id_runner, id, className, param, isEnabled, priority) values
(3, 31, 'ru.taximaxim.dbreplicator2.replica.strategies.errors.CountWatchgdog', 'where=c_date<DATEADD(''SECOND'', -5, NOW())', true, 100);
