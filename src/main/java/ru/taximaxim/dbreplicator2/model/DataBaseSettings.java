/* The MIT License (MIT)
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

package ru.taximaxim.dbreplicator2.model;

/**
 * Интерфейс настроек подключения к БД
 *
 * @author volodin_aa
 *
 */
public interface DataBaseSettings {

    /**
     * Получение имени пула
     *
     * @return - имя пула
     */
    String getPoolId();

    /**
     * Установка имени пула
     *
     * @param poolId
     *            - имя пула
     */
    void setPoolId(String poolId);

    /**
     * Получение имени драйвера БД
     *
     * @return - имя драйвера БД
     */
    String getDriver();

    /**
     * Установка имени драйвра БД
     *
     * @param driver
     *            - имя драйвера БД
     */
    void setDriver(String driver);

    /**
     * Получение строки подключения к БД
     *
     * @return - строка подключения к БД
     */
    String getUrl();

    /**
     * Установка строки подключения к БД
     *
     * @param url
     *            - строка подключения к БД
     */
    void setUrl(String url);

    /**
     * Получение имени пользователя
     *
     * @return - имя пользователя
     */
    String getUser();

    /**
     * Установка имени пользователя
     *
     * @param user
     *            - имя пользователя
     */
    void setUser(String user);

    /**
     * Получение пароля
     *
     * @return - пароль
     */
    String getPass();

    /**
     * Установка пароля
     *
     * @param pass
     *            - пароль
     */
    void setPass(String pass);

}
