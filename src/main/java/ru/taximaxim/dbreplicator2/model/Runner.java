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
package ru.taximaxim.dbreplicator2.model;

import java.util.List;

/**
 * Интерфейс раннера
 * 
 * @author volodin_aa
 *
 */
public interface Runner {

    /**
     * Настройки пула соединений базы-источника
     *
     * @return Строковый идентификатор пула соединений базы-истончка
     */
    BoneCPSettingsModel getSource();

    /**
     * Настройки пула соединений целевой БД
     *
     * @return Строковый идентификатор пула соединений целевой БД
     */
    BoneCPSettingsModel getTarget();

    /**
     * Идентификатор потока-реплики
     *
     * @return Идентификатор потока-реплики
     */
    Integer getId();

    /**
     * Описание потока реплики
     *
     * @return Описание потока реплики
     */
    String getDescription();

    /**
     * Список упорядоченных по приоритету разрешенных стратегий для потока
     * реплики.
     *
     * @return Список упорядоченных по приоритету разрешенных стратегий для
     *         потока реплики.
     */
    List<StrategyModel> getStrategyModels();

    /**
     * Класс обработчика
     *
     * @return имя класса обработчика
     */
    String getClassName();
}
