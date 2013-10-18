/* The MIT License (MIT)
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

/**
 * 
 */
package ru.taximaxim.dbreplicator2.ConnectionsFactory;

/**
 * Интерфейс к настройкам подключения в BoneCP
 * 
 * @author volodin_aa
 *
 */
public interface BoneCPSettings extends DataBaseSettings {

    /**
     * Получение минимального количества соединений в пуле
     * 
     * @return - количество соединений
     */
    public int getMinConnectionsPerPartition();

    /**
     * Установка минимального количества соединений в пуле
     * 
     * @param minConnectionsPerPartition - количество соединений
     */
    public void setMinConnectionsPerPartition(int minConnectionsPerPartition);

    /**
     * Получение максимального количества соединений в пуле
     * 
     * @return - количество соединений
     */
    public int getMaxConnectionsPerPartition();

    /**
     * Установка максимального количества соединений в пуле
     * 
     * @param maxConnectionsPerPartition - количество соединений
     */
    public void setMaxConnectionsPerPartition(int maxConnectionsPerPartition);

    /**
     * Получение количесва порций соединений в пуле
     * 
     * @return - количество порций
     */
    public int getPartitionCount();

    /**
     * Установка количесва порций соединений в пуле
     * 
     * @param partitionCount - количество порций
     */
    public void setPartitionCount(int partitionCount);

    /**
     * Установка таймаута на ожидание получения соединения из пула
     * 
     * @return - таймаут, мс
     */
    public long getConnectionTimeoutInMs();

    /**
     * Получение таймаута на ожидание получения соединения из пула
     * 
     * @param connectionTimeoutInMs - таймаут, мс
     */
    public void setConnectionTimeoutInMs(long connectionTimeoutInMs);

    /**
     * Установка таймаута на ожидание закрытия соединения
     * 
     * @return - таймаут, мс
     */
    public long getCloseConnectionWatchTimeoutInMs();

    /**
     * Получение таймаута на ожидание закрытия соединения
     * 
     * @param closeConnectionWatchTimeoutInMs - таймаут, мс
     */
    public void setCloseConnectionWatchTimeoutInMs(long closeConnectionWatchTimeoutInMs);

}
