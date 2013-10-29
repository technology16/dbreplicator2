INSERT INTO T_Source (_int, _boolean, _long, _decimal, _double, _float, _string, _byte, _date) VALUES(1, true, 5968326496, 99.65, 5.62, 79.6, 'Delete', 0, now());
INSERT INTO T_Source (_int, _boolean, _long, _decimal, _double, _float, _string, _byte, _date) VALUES(2, false, 596328794, 45.92, 7.16, 21.2, 'Update', 1, now());
INSERT INTO T_Source (_int, _boolean, _long, _decimal, _double, _float, _string, _byte, _date) VALUES(3, true, 7963256489, 75.16, 9.15, 33.9, 'Insert', 2, now());
UPDATE T_Source SET _boolean = true, _long = 0, _decimal = 0, _double = 0, _float = 0, _string = 'Update', _byte = 0, _date = now() WHERE _int = 2;
DELETE FROM T_Source WHERE _int = 1;
