/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2013 Technologiya
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package ru.taximaxim.dbreplicator2;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Date;

public class MyTablesType {

    /**
     * IDENTITY
     */
    public int id;
    /**
     * INT  
     * INTEGER
     * MEDIUMINT
     * INT4
     * SIGNED
     */
    public Integer _int;
    /**
     * BOOLEAN
     */
    public boolean _boolean;
    /**
     * BIGINT
     * INT8
     */
    public Long _long;
    /**
     * DECIMAL  
     * NUMBER
     * DEC
     * NUMERIC 
     */
    public BigDecimal _decimal; 
    /**
     * DOUBLE 
     * FLOAT   
     * FLOAT4  
     * FLOAT8
     */
    public double _double;
    /**
     * REAL
     */
    public float _float;
    /**
     * VARCHAR  
     * LONGVARCHAR
     * VARCHAR2
     * NVARCHAR
     * NVARCHAR2
     * VARCHAR_CASESENSITIVE   
     */
    public String _string;
    /**
     * BINARY
     * VARBINARY
     * LONGVARBINARY
     * RAW
     * BYTEA
     */
    public byte _byte;
    /**
     * DATE
     */
    public Date _date;
    /**
     * TIME
     */
    public Time _time;
    /**
     * TIMESTAMP  
     * DATETIME
     * SMALLDATETIME   
     */
    public Timestamp _timestamp;
}
