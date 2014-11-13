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

import java.io.IOException;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractSettingTest;

public class TriggerH2Test extends AbstractSettingTest {
    
    protected static final Logger LOG = Logger.getLogger(TriggerH2Test.class);
    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("importRep2.sql", "importSource.sql", "importSourceData.sql");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        close();
    }
    
    /**
     * Проверка тригерров
     * @throws SQLException
     * @throws ClassNotFoundException
     * @throws IOException
     * @throws InterruptedException 
     */
    @Test
    public void testTrigger() throws SQLException, InterruptedException {     
        int countT_TABLE = Helper.InfoCount(conn, "t_table");
        if(countT_TABLE==0) {
            LOG.error("Таблице t_table не должна пустой: count = " + countT_TABLE);
        }
        Assert.assertNotEquals(countT_TABLE, 0);
        
        int countrep2_superlog = Helper.InfoCount(conn, "rep2_superlog");
        if(countrep2_superlog==0) {
            LOG.error("Таблица rep2_superlog не должна пустой: count = " + countrep2_superlog);
        }
        Assert.assertNotEquals(countrep2_superlog, 0);
        
        LOG.info("<====== t_table ======>");
        Helper.InfoSelect(conn, "t_table");
        LOG.info(">====== t_table ======<");
        
        LOG.info("<====== rep2_superlog ======>");
        Helper.InfoSelect(conn, "rep2_superlog");
        LOG.info("======= rep2_superlog =======");
    }
}
