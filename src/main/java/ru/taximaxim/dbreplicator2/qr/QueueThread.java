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
package ru.taximaxim.dbreplicator2.qr;

import java.util.Collection;

import ru.taximaxim.dbreplicator2.model.Runner;
import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.tp.WorkerThread;

/**
 * Ожидающий рабочий поток. При запуске он удаляет раннера из набора
 * ожидающих обработки раннеров.
 * Реализует паттерн Proxy для интерфейса Runnable.
 * 
 * @author volodin_aa
 *
 */
public class QueueThread implements Runnable {

    private Collection<RunnerModel> workRunners;
    private Runner runner;
    private WorkerThread workerThread;

    /**
     * Конструктор по списку ожидающих раннеров и раннеру
     * 
     * @param workRunners - список ожидающих обработки раннеров
     * @param runner - текущий раннер
     */
    public QueueThread(Runner runner, Collection<RunnerModel> workRunners) {
        this.workRunners = workRunners;
        this.runner = runner;
    }

    @Override
    public void run() {
        try {
            getWorkerThread().run();
        }finally {
            synchronized(workRunners) {
                workRunners.remove(runner);
            }
        }
    }

    /**
     * Метод с отложенной инициализацией потока обработчика раннера
     * 
     * @return the workerThread - поток обработчика раннера
     */
    protected synchronized WorkerThread getWorkerThread() {
        if (workerThread==null) {
            workerThread = new WorkerThread(runner);
        }
        return workerThread;
    }

}
