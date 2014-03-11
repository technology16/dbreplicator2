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
package ru.taximaxim.dbreplicator2.el;

import java.lang.Thread.UncaughtExceptionHandler;

import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Обработчик по умолчанию необработанных исключений потока
 * 
 * @author volodin_aa
 *
 */
public class DefaultUncaughtExceptionHandler implements UncaughtExceptionHandler {

    /**
     * Идентификатор раннера 
     */
    private Integer runnerId;
    
    /**
     * Получение раннера потока
     * 
     * @return the runnerId
     */
    protected Integer getRunnerId() {
        return runnerId;
    }

    /**
     * Конструктор для конкретного раннера
     * 
     * @param runnerId
     */
    public DefaultUncaughtExceptionHandler(Integer runnerId) {
        this.runnerId = runnerId;
    }

    /**
     * Конструктор по умолчанию
     */
    public DefaultUncaughtExceptionHandler() {
    }

    /* (non-Javadoc)
     * @see java.lang.Thread.UncaughtExceptionHandler#uncaughtException(java.lang.Thread, java.lang.Throwable)
     */
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        // В случае непредвиденной остановки потока пишем в лог сообщение об ошибке
        try (ErrorsLog errorsLog = Core.getErrorsLog()) {
            errorsLog.add(getRunnerId(), null, null, "Непредвиденная ошибка потока: ", e);
        }
    }

}
