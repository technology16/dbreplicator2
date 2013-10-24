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

package ru.taximaxim.dbreplicator2;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import ru.taximaxim.dbreplicator2.model.RunnerModel;

public class ThreadPoolTest {

    private static ThreadPool threadPool = null;
    private static int count = 3;
    private static int id = 0;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        threadPool = new ThreadPool(count);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        threadPool.shutdown();
    }

    @Test
    public void testPool() {

        threadPool.start(createRunner("source", "target", "Description"));
        threadPool.start(createRunner("source", "target", "Описание"));
        threadPool.start(createRunner("source", "target", "Модуль"));
        threadPool.start(createRunner("source", "target", "Поток"));
        threadPool.start(createRunner("source", "target", "Тест"));
        threadPool.start(createRunner("source", "target", "Привет"));
        ;
        threadPool.start(createRunner("source", "target", "Пул"));
        threadPool.start(createRunner("source", "target", "Запуск"));
        threadPool.start(createRunner("source", "target", "Пуск"));
        threadPool.start(createRunner("source", "target", "Модель"));
        threadPool.start(createRunner("source", "target", "java"));
        threadPool.start(createRunner("source", "target", "info"));
        threadPool.start(createRunner("source", "target", "Салют"));
    }

    public RunnerModel createRunner(String source, String target,
            String description) {

        RunnerModel runner = new RunnerModel();

        runner.setId(id++);
        runner.setSource(source);
        runner.setTarget(target);
        runner.setDescription(description);

        return runner;
    }
}
