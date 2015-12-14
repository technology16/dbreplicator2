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

/**
 * Интерфейс настроек задачи по расписанию
 * 
 * @author volodin_aa
 *
 */
public interface TaskSettings {

    /**
     * Получение идентификатора задачи
     *
     * @return
     */
    int getTaskId();

    /**
     * Установка идентификатора задачи
     *
     * @param taskId
     */
    void setTaskId(int taskId);

    /**
     * Получение флага доступности задачи
     *
     * @return
     */
    boolean getEnabled();

    /**
     * Установка флага доступности
     *
     * @param enabled
     */
    void setEnabled(boolean enabled);

    /**
     * Описание задачи
     *
     * @return Описание задачи
     */
    String getDescription();

    /**
     * Описание задачи
     *
     * @param description Описание задачи
     */
    void setDescription(String description);

    /**
     * Функция получения инициализированного экземпляра обработчика реплики
     *
     * @return
     */
    RunnerModel getRunner();

    /**
     * Установка инициализированного экземпляра обработчика реплики
     *
     * @param runner
     */
    void setRunner(RunnerModel runner);

    /**
     * Получение cron строки с описанием периодичночти запуска задачи
     * @return
     */
    String getCronString();

    /**
     * Установка cron строки с описанием периодичночти запуска задачи
     * @param cronString
     */
    void setCronString(String cronString);
}
