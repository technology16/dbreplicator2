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
import java.util.Iterator;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.model.Runner;
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
    
    private static final Logger LOG = Logger.getLogger(QueueThread.class);

    private Collection<Runner> awaitRunners;
    private Collection<Runner> workRunners;
    
    /**
     * Конструктор по списку ожидающих раннеров и раннеру
     * 
     * @param workRunners - список ожидающих обработки раннеров
     * @param runner - текущий раннер
     */
    public QueueThread(Collection<Runner> awaitRunners, Collection<Runner> workRunners) {
        this.awaitRunners = awaitRunners;
        this.workRunners = workRunners;
    }

    /**
     * Выбирает раннер для запуска на обработку
     * @return
     * @throws InterruptedException 
     */
    protected Runner getRunner(){
        synchronized(awaitRunners) {
            Iterator<Runner> it = awaitRunners.iterator();
            while (it.hasNext()) {
                Runner awaitRun = it.next();
                try {
                    synchronized(workRunners) {
                        if (!workRunners.contains(awaitRun)) {
                            workRunners.add(awaitRun);
                            it.remove();
                            return awaitRun;
                        }
                    }
                } finally {
                    fireRunner(awaitRun);
                }
            }
        }

        return null;
    }

    /**
     * Удаляет раннер из коллекции обрабатывающихся раннеров
     * @param runner
     */
    protected void fireRunner(Runner runner){
        if (runner != null) {
            synchronized(workRunners) {
                workRunners.remove(runner);
            }
        }
    }
    
    /**
     * Запускает раннер на обработку
     */
    @Override
    public void run() {
        while (true) {
            Runner runner = this.getRunner();
                while (runner != null) {
                    try {
                        LOG.warn("Work runner " + runner.getId());
                        WorkerThread worker = new WorkerThread(runner);
                        worker.run();
                    } finally {
                        this.fireRunner(runner);
                    }
                    runner = this.getRunner();
                }
            try {
                LOG.warn("awaitRunners.wait()");
                
                synchronized (awaitRunners) {
                    awaitRunners.wait();
                }
            } catch (InterruptedException e1) {

            }
        }
    }
}
