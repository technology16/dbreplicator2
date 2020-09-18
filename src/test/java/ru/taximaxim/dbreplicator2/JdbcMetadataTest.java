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

import junit.framework.TestCase;
import ru.taximaxim.dbreplicator2.jdbc.JdbcMetadata;

/**
 * Класс для тестирования работы с метаданными
 *
 * @author volodin_aa
 *
 */
public class JdbcMetadataTest extends TestCase {

    /**
     * Тест получения имени таблицы из строки с полным именем таблицы
     */
    public void testGetTableName() {
        assertEquals("Ошибка при получении имени таблицы T_TABLE из T_TABLE",
                "T_TABLE", JdbcMetadata.getTableName("T_TABLE"));

        assertEquals("Ошибка при получении имени таблицы T_TABLE из schema.T_TABLE",
                "T_TABLE", JdbcMetadata.getTableName("schema.T_TABLE"));
    }

    /**
     * Тест получения схемы таблицы из строки с полным именем таблицы
     */
    public void testGetSchemaName() {
        assertNull("Ошибка при получении пустого имени схемы из T_TABLE",
                JdbcMetadata.getSchemaName("T_TABLE"));

        assertEquals("Ошибка при получении имени схемы schema из schema.T_TABLE",
                "schema", JdbcMetadata.getSchemaName("schema.T_TABLE"));
    }
}
