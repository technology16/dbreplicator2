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

package ru.taximaxim.dbreplicator2.tp;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.model.RunnerModel;
/**
 * Пул потоков
 * 
 * @author volodin_aa
 *
 */
public class ThreadPool {

    /**
     * Супер логгер
     */
    private static final Logger LOG = Logger.getLogger(ThreadPool.class);

    private ExecutorService executor = null;
    private Set<RunnerModel> pendingRunners = new HashSet<RunnerModel>();

    /**
     * Инициализация пула потоков
     * 
     * @param count - максимальное количество активных потоков
     * @throws InterruptedException
     */
    public ThreadPool(int count) { 
        executor = Executors.newFixedThreadPool(count);

        LOG.info(String.format("Создание и запуск пула потоков (%s)", count));
    }

    /**
     * Ожидание остановки выполнения всех потоков и завершение работы.
     * @throws InterruptedException 
     */
    public void shutdownThreadPool() throws InterruptedException {
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
    public final void restartThreadPool(int count) throws InterruptedException {
        shutdownThreadPool();
        executor = Executors.newFixedThreadPool(count);

        LOG.info(String.format("Перезапуск пула потоков (%s)", count));
    }

    /**
     * Запуск потока RunnerModel. Поток будет поставлен в очередь на выполнение.
     */
    public void start(RunnerModel runner) {
        synchronized(pendingRunners) {
            // Если нет ожидающих потоков, то добавляем поток на обработку раннера
            if (!pendingRunners.contains(runner)) {
                pendingRunners.add(runner);
                Runnable worker = new PendingWorkerThread(runner, pendingRunners);
                executor.execute(worker);
            }
        }
    }
}
