INSERT INTO t_table (_int, _boolean, _long, _decimal, _double, _float, _string, _byte, _date, _time, _timestamp) VALUES(2, false, 596328794, 45.92, 7.16, 21.2, 'Insert', 1, now(), now(), now());
INSERT INTO t_table1 (_int, _boolean, _long, _decimal, _double, _float, _string, _byte, _date, _time, _timestamp) VALUES(1, true, 5968326496, 99.65, 5.62, 79.6, 'Insert', 0, now(), now(), now());
INSERT INTO t_table2 (_int, _boolean, _long, _decimal, _double, _float, _string, _byte, _date, _time, _timestamp) VALUES(2, false, 596328794, 45.92, 7.16, 21.2, 'Insert', 1, now(), now(), now());
INSERT INTO t_table3 (_int, _boolean, _long, _decimal, _double, _float, _string, _byte, _date, _time, _timestamp) VALUES(1, true, 5968326496, 99.65, 5.62, 79.6, 'Insert', 0, now(), now(), now());
INSERT INTO t_table4 (_int, _boolean, _long, _decimal, _double, _float, _string, _byte, _date, _time, _timestamp) VALUES(1, true, 5968326496, 99.65, 5.62, 79.6, 'Insert', 0, now(), now(), now());
INSERT INTO t_table5 (_int, _boolean, _long, _decimal, _double, _float, _string, _byte, _date, _time, _timestamp) VALUES(2, false, 596328794, 45.92, 7.16, 21.2, 'Insert', 1, now(), now(), now());
---
UPDATE t_table SET _boolean = true, _long = null, _decimal = null, _double = null, _float = null, _string = null, _byte = null, _date = null, _time = null, _timestamp = null WHERE _int = 1;
UPDATE t_table1 SET _boolean = true, _long = null, _decimal = null, _double = null, _float = null, _string = null, _byte = null, _date = null, _time = null, _timestamp = null WHERE _int = 2;
UPDATE t_table2 SET _boolean = true, _long = null, _decimal = null, _double = null, _float = null, _string = null, _byte = null, _date = null, _time = null, _timestamp = null WHERE _int = 1;
UPDATE t_table3 SET _boolean = true, _long = null, _decimal = null, _double = null, _float = null, _string = null, _byte = null, _date = null, _time = null, _timestamp = null WHERE _int = 2;
UPDATE t_table4 SET _boolean = true, _long = null, _decimal = null, _double = null, _float = null, _string = null, _byte = null, _date = null, _time = null, _timestamp = null WHERE _int = 1;
UPDATE t_table5 SET _boolean = true, _long = null, _decimal = null, _double = null, _float = null, _string = null, _byte = null, _date = null, _time = null, _timestamp = null WHERE _int = 2;
