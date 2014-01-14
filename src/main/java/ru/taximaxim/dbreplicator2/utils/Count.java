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

package ru.taximaxim.dbreplicator2.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Счетчик
 * 
 * @author mardanov_rm
 */
public class Count {
    
    Map<String,Integer> tablesSuccess = new HashMap<String,Integer>();
    
    Map<String,Integer> tablesError = new HashMap<String,Integer>();
    
    /**
     * Увеличение счетчика успешных операций на ед.
     * 
     * @param tableName - имя таблицы
     */
    public void addSuccess(String tableName) {
        Integer count = 1;
        if(tablesSuccess.get(tableName) != null) {
            count = tablesSuccess.get(tableName) + 1;
        }
        tablesSuccess.put(tableName, count);
    }
    
    /**
     * Увеличение счетчика ошибочных операций на ед.
     * 
     * @param tableName - имя таблицы
     */
    public void addError(String tableName){ 
        Integer count = 1;
        if(tablesError.get(tableName) != null) {
            count = tablesError.get(tableName) + 1;
        }
        tablesError.put(tableName, count);
    }
    
    /**
     * Получение таблиц успешных операций
     * @return
     */
    public Set<String> getSuccessTables(){
        return tablesSuccess.keySet();
    }
    
    /**
     * Получение счетчика успешных операций
     * @return
     */
    public int getSuccess(String tableName){
        return tablesSuccess.get(tableName);
    }
    
    /**
     * Получение таблиц ошибочных операций
     * @return
     */
    public Set<String> getErrorTables(){
        return tablesError.keySet();
    }

    /**
     * Получение счетчика ошибочных операций
     * @return
     */
    public int getError(String tableName){
        return tablesError.get(tableName);
    }
}
