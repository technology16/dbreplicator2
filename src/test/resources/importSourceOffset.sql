drop table if exists T_TABLE;
drop table if exists T_TABLE1;
drop table if exists T_TABLE2;
drop table if exists T_TABLE3;
drop table if exists T_TABLE4;
drop table if exists T_TABLE5;
drop table if exists T_TAB;

CREATE TABLE T_TAB (ID IDENTITY PRIMARY KEY, _value VARCHAR(250));
INSERT INTO T_TAB (_value) VALUES('');

CREATE TABLE T_TABLE (ID IDENTITY PRIMARY KEY, _int INT, _boolean BOOLEAN, _long BIGINT, _decimal DECIMAL, _double DOUBLE, _float REAL, _string VARCHAR(250), _byte BINARY, _date DATE, _time TIME, _timestamp TIMESTAMP);
CREATE TABLE T_TABLE1 (ID IDENTITY PRIMARY KEY, _int INT, _boolean BOOLEAN, _long BIGINT, _decimal DECIMAL, _double DOUBLE, _float REAL, _string VARCHAR(250), _byte BINARY, _date DATE, _time TIME, _timestamp TIMESTAMP);
--
CREATE TABLE T_TABLE2 (ID IDENTITY PRIMARY KEY, _int INT, _boolean BOOLEAN, _long BIGINT, _decimal DECIMAL, _double DOUBLE, _float REAL, _string VARCHAR(250), _byte BINARY, _date DATE, _time TIME, _timestamp TIMESTAMP);
CREATE TABLE T_TABLE3 (ID IDENTITY PRIMARY KEY, _int INT, _boolean BOOLEAN, _long BIGINT, _decimal DECIMAL, _double DOUBLE, _float REAL, _string VARCHAR(250), _byte BINARY, _date DATE, _time TIME, _timestamp TIMESTAMP);
--
CREATE TABLE T_TABLE4 (ID IDENTITY PRIMARY KEY, _int INT, _boolean BOOLEAN, _long BIGINT, _decimal DECIMAL, _double DOUBLE, _float REAL, _string VARCHAR(250), _byte BINARY, _date DATE, _time TIME, _timestamp TIMESTAMP);
CREATE TABLE T_TABLE5 (ID IDENTITY PRIMARY KEY, _int INT, _boolean BOOLEAN, _long BIGINT, _decimal DECIMAL, _double DOUBLE, _float REAL, _string VARCHAR(250), _byte BINARY, _date DATE, _time TIME, _timestamp TIMESTAMP);

--<#CreateTrigger#>T_TABLE,T_TABLE1,T_TABLE2,T_TABLE3,T_TABLE4,T_TABLE5<#CreateTrigger#>
