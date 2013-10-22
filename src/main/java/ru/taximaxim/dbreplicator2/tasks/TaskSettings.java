package ru.taximaxim.dbreplicator2.tasks;

public interface TaskSettings {

    /**
     * Получение идентификатора задачи
     * 
     * @return
     */
    public int getTaskId();
    
    /**
     * Установка идентификатора задачи
     * 
     * @param taskId
     */
    public void setTaskId(int taskId);
    
    /**
     * Получение идентификатора запускаемой реплики
     * 
     * @return
     */
    public int getReplicaId();
    
    /**
     * Установка идентификатора запускаемой реплики
     * 
     * @param replicaId
     */
    public void setReplicaId(int replicaId);
    
    /**
     * Получение приоритета задачи, задачи выполняются в порядке возрастания приоритета
     * 
     * @return
     */
    public int getPriority();
    
    /**
     * Установка приоритетазадачи
     * 
     * @param priority
     */
    public void setPriority(int priority);
    
    /**
     * Получение флага доступности задачи
     * 
     * @return
     */
    public boolean getEnabled();
    
    /**
     * Установка флага доступности
     * 
     * @param enabled
     */
    public void setEnabled(boolean enabled);
    
    /**
     * Получение интервала ожидания после корректного завершения задачи, мс
     * 
     * @return
     */
    public int getSuccessInterval();
    
    /**
     * Установка интервала ожидания после корректного завершения задачи, мс
     * 
     * @param successInterval
     */
    public void setSuccessInterval(int successInterval);
    
    /**
     * Получение интервала ожидания после ошибочного завершения задачи, мс
     * 
     * @return
     */
    public int getFailInterval();
    
    /**
     * Установка интервала ожидания после ошибочного завершения задачи, мс
     * 
     * @param failInterval
     */
    public void setFailInterval(int failInterval);
    
}
