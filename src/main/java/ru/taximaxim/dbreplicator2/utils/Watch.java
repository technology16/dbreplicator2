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

package ru.taximaxim.dbreplicator2.utils;

import java.util.Date;

/**
 * Класс для отслеживания равных промежутков времени
 * 
 * 
 * @author volodin_aa
 *
 */
public class Watch {

    private final Date date;
    private long startTime;

    /**
     * Конструктор 
     */
    public Watch() {
        this.date = new Date();
        startTime = date.getTime();
    }

    /**
     * Вычисляем время до окончания интервала
     *
     * @param interval
     *            отслеживаемый интервал, мс
     */
    public long remaining(long interval) {
        return startTime + interval - date.getTime();
    }

    /**
     * Проверяем окончание интервала
     *
     * @param interval
     *            отслеживаемый интервал, мс
     */
    public boolean timeOut(long interval) {
        return remaining(interval) <= 0;
    }

    /**
     * Засыпаем до окончания выделенного интервала времени
     * 
     *
     * @param interval
     *            отслеживаемый интервал, мс
     * @throws InterruptedException
     */
    public void sleep(long interval) throws InterruptedException {
        long sleepTime = remaining(interval);
        if (sleepTime > 0) {
            Thread.sleep(sleepTime);
        }
        start();
    }

    /**
     * Отмеряем начало интервала
     */
    public void start() {
        startTime = date.getTime();
    }
}
