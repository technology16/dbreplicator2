drop table if exists T_TABLE;
drop table if exists T_TABLE1;
drop table if exists T_TABLE2;
drop table if exists T_TABLE3;
drop table if exists T_TABLE4;
drop table if exists T_TABLE5;

CREATE TABLE T_TABLE (ID IDENTITY PRIMARY KEY, _int INT, _boolean BOOLEAN, _long BIGINT, _decimal DECIMAL, _double DOUBLE, _float REAL, _string VARCHAR(250), _byte BINARY, _date DATE, _time TIME, _timestamp TIMESTAMP);
CREATE TABLE T_TABLE1 (ID IDENTITY PRIMARY KEY, _int INT, _boolean BOOLEAN, _long BIGINT, _decimal DECIMAL, _double DOUBLE, _float REAL, _string VARCHAR(250), _byte BINARY, _date DATE, _time TIME, _timestamp TIMESTAMP);
--
CREATE TABLE T_TABLE2 (ID IDENTITY PRIMARY KEY, _int INT, _boolean BOOLEAN, _long BIGINT, _decimal DECIMAL, _double DOUBLE, _float REAL, _string VARCHAR(250), _byte BINARY, _date DATE, _time TIME, _timestamp TIMESTAMP);
CREATE TABLE T_TABLE3 (ID IDENTITY PRIMARY KEY, _int INT, _boolean BOOLEAN, _long BIGINT, _decimal DECIMAL, _double DOUBLE, _float REAL, _string VARCHAR(250), _byte BINARY, _date DATE, _time TIME, _timestamp TIMESTAMP);
ALTER TABLE T_TABLE3 ADD FOREIGN KEY (_int) REFERENCES T_TABLE2(ID);
--
CREATE TABLE T_TABLE4 (ID IDENTITY PRIMARY KEY, _int INT, _boolean BOOLEAN, _long BIGINT, _decimal DECIMAL, _double DOUBLE, _float REAL, _string VARCHAR(250), _byte BINARY, _date DATE, _time TIME, _timestamp TIMESTAMP);
CREATE TABLE T_TABLE5 (ID IDENTITY PRIMARY KEY, _int INT, _boolean BOOLEAN, _long BIGINT, _decimal DECIMAL, _double DOUBLE, _float REAL, _string VARCHAR(250), _byte BINARY, _date DATE, _time TIME, _timestamp TIMESTAMP);
