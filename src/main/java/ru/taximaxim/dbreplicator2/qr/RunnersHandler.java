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
import java.util.Iterator;

import ru.taximaxim.dbreplicator2.model.RunnerModel;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * Обработчик очереди раннеров
 * 
 * @author petrov_im
 * 
 */
public class RunnersHandler extends Observer{
    
    public RunnersHandler(RunnersQueue runnersQueue) {
        this.runnersQueue = runnersQueue;
        this.runnersQueue.attach(this);
    }
    
    @Override
    public void update() throws InterruptedException {
              
        
        ThreadPoolQueue threadPool = Core.getThreadPoolQueue();
        Collection<RunnerModel> awaitRunners = runnersQueue.getQueueRunners();
        Collection<RunnerModel> workRunners = threadPool.getWorkRunners();
        synchronized(awaitRunners) {
            synchronized(workRunners) {
                if (threadPool.getCount() > workRunners.size()) {
                    Iterator<RunnerModel> it = awaitRunners.iterator();
                    while (it.hasNext()) {
                        RunnerModel awaitRun = it.next();
                        it.remove();
                        workRunners.add(awaitRun);
                        threadPool.start(awaitRun);
                    }
                    /*for (RunnerModel awaitRun : awaitRunners) {
                        if (!workRunners.contains(awaitRun) && threadPool.getCount()>workRunners.size()) {
                            awaitRunners.remove(awaitRun);
                            workRunners.add(awaitRun);
                            threadPool.start(awaitRun);
                        }
                    }*/
                }
            }
        }       
    }
}
