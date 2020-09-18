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

import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.abstracts.AbstractSettingTest;

public class TriggerH2Test extends AbstractSettingTest {

    protected static final Logger LOG = Logger.getLogger(TriggerH2Test.class);

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        setUp("init_db/importRep2.sql", "init_db/importSource.sql", "init_db/importSourceData.sql");
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        close();
    }

    /**
     * Проверка тригерров
     * @throws SQLException
     * @throws InterruptedException
     */
    @Test
    public void testTrigger() throws SQLException, InterruptedException {
        Helper.assertNotEmptyTable(conn, "t_table");
        Helper.assertNotEmptyTable(conn, "rep2_superlog");

        LOG.info("<====== t_table ======>");
        Helper.InfoSelect(conn, "t_table");
        LOG.info(">====== t_table ======<");

        LOG.info("<====== rep2_superlog ======>");
        Helper.InfoSelect(conn, "rep2_superlog");
        LOG.info("======= rep2_superlog =======");
    }
}
