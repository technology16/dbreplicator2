/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2014 Technologiya
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
import java.util.LinkedHashSet;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.model.Runner;
import ru.taximaxim.dbreplicator2.model.RunnerModel;

/**
 * Очередь раннеров ожидающих обработку
 * 
 * @author petrov_im
 * 
 */
public class RunnersQueue {
    
    private static final Logger LOG = Logger.getLogger(RunnersQueue.class);

    private Collection<Runner> queueRunners = new LinkedHashSet<Runner>();
    
    /**
     * Добавляет поступившие из суперлога раннеры в очередь на обработку
     * 
     * @param runners
     * @throws InterruptedException
     */
    public void addAll(Collection<RunnerModel> runners) throws InterruptedException {
        synchronized (queueRunners) {
            queueRunners.addAll(runners);
            queueRunners.notifyAll();
            LOG.warn("queueRunners.notifyAll()");
        }
    }
    
    /**
     * Возвращает очередь раннеров, ожидающих обработки
     * 
     * @return
     */
    public Collection<Runner> getQueueRunners() {
        return queueRunners;
    }
}
