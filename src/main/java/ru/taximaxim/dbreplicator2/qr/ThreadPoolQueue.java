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
import java.util.LinkedHashSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.model.Runner;
/**
 * Пул потоков
 * 
 * @author petrov_im
 *
 */
public class ThreadPoolQueue {

    /**
     * Супер логгер
     */
    private static final Logger LOG = Logger.getLogger(ThreadPoolQueue.class);

    private ExecutorService executor = null;
    private Collection<Runner> workRunners = new LinkedHashSet<Runner>();
    private RunnersQueue awaitRunners;
    private int count;

    /**
     * Инициализация пула потоков
     * 
     * @param count - максимальное количество активных потоков
     * @throws InterruptedException
     */
    public ThreadPoolQueue(int count, RunnersQueue awaitRunners) throws InterruptedException {
        this.count = count;
        executor = Executors.newFixedThreadPool(count);
        
        this.awaitRunners = awaitRunners;
        
        for (int i=0; i<count; i++){
            this.start();
        }
        
        LOG.info(String.format("Создание и запуск пула потоков (%s)", count));
    }

    /**
     * Ожидание остановки выполнения всех потоков и завершение работы.
     * @throws InterruptedException 
     */
    public void shutdownThreadPoolQueue() throws InterruptedException {
        if (executor != null) {
            executor.shutdown();
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            executor = null;

            LOG.info("ThreadPool.shutdown()");
        }
    }

    /**
     * Ожидание завершения всех потоков и инициализация нового пула с новым
     * значением.
     * @throws InterruptedException 
     */
    public final void restartThreadPoolQueue(int count) throws InterruptedException {
        shutdownThreadPoolQueue();
        executor = Executors.newFixedThreadPool(count);

        LOG.info(String.format("Перезапуск пула потоков (%s)", count));
    }

    /**
     * Запуск потока RunnerModel. Поток будет поставлен в очередь на выполнение.
     */
    public void start() {
        Runnable worker = new QueueThread(awaitRunners.getQueueRunners(), workRunners);
        executor.execute(worker);
        
    }
    
    /**
     * Возвращает максимальное возможное количество потоков
     * @return
     */
    public int getCount(){
        return this.count;
    }
    
    /**
     * Возвращает число обрабатывающихся раннеров
     * @return
     */
    public int getWorkRunnersCount(){
        synchronized(workRunners) {
            return workRunners.size();
        }
    }
}