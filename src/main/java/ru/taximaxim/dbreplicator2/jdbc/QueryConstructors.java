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

/**
 * Функции для конструирования запросов
 * 
 */
package ru.taximaxim.dbreplicator2.jdbc;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author volodin_aa
 * 
 */
public final class QueryConstructors {
    
    private static final String QUESTION = "?";
    private static final String DELIMITER = ", ";
    private static final String INSERT_INTO = "INSERT INTO ";
    private static final String VALUES = ") VALUES (";
    private static final String SELECT = "SELECT ";
    private static final String FROM = " FROM ";
    private static final String WHERE = " WHERE ";
    private static final String ORDER_BY = " ORDER BY ";
    private static final String AND = " AND ";
    private static final String DELETE_FROM = "DELETE FROM ";
    private static final String UPDATE = "UPDATE ";
    private static final String SET = " SET ";
    
    /**
     * Сиглетон
     */
    private QueryConstructors() {}

    public static Collection<String> listAddPostfix(Collection<?> list, Map<String, String> castCols,
            String postfix) {
        
        Collection<String> newList = new ArrayList<String>();
        for (Object val : list) {
            if (castCols.keySet().contains(val)) {
                newList.add(val + "=" + castCols.get(val));
            } else {
                newList.add(val + postfix);
            }
        }
        return newList;
    }
    
    public static Collection<String> listToCastString(Collection<?> list, Map<String, String> castCols) {
        Collection<String> newList = new ArrayList<String>();
        for (Object val : list) {
            if (castCols.keySet().contains(val)) {
                newList.add(castCols.get(val));
            } else {
                newList.add(val.toString());
            }
        }
        return newList;
    }
    
    /**
     * Строит строку из элементов списка, разделенных разделителем delimiter
     * 
     * @param list
     *            список объектов
     * @param delimiter
     *            разделитель
     * @return строка из элементов списка, разделенных разделителем delimiter
     */
    public static String listToString(Collection<?> list, String delimiter) {
        StringBuffer result = new StringBuffer();
        boolean setComma = false;
        for (Object val : list) {
            if (setComma) {
                result.append(delimiter);
            } else {
                setComma = true;
            }
            result.append(val);
        }
        return result.toString();
    }
    
    /**
     * Строит строку из элементов списка с добавленным postfix в конце и
     * разделенных разделителем delimiter
     * 
     * @param list
     *            список объектов
     * @param delimiter
     *            разделитель
     * @param postfix
     *            строка для добавления после строки элемента
     * @return строка из элементов списка с добавленным postfix в конце и
     *         разделенных разделителем delimiter
     */
    public static String listToString(Collection<?> list, String delimiter, String postfix) {
        return listToString(listAddPostfix(list, new HashMap<String, String>(), postfix), delimiter);
    }
    
    /**
     * Строит строку из элементов списка, разделенных разделителем delimiter
     * с заменой значений колонок выражениями из карты подстановки
     * 
     * @param list
     *            список объектов
     * @param castCols
     *            карта подстановки вместо колонок
     * @param delimiter
     *            разделитель
     * @return строка из элементов списка, разделенных разделителем delimiter
     */
    public static String listToString(Collection<?> list, Map<String, String> castCols, String delimiter) {
        return listToString(listToCastString(list, castCols), delimiter);
    }
    
    /**
     * Строит строку из элементов списка с добавленным postfix в конце и
     * разделенных разделителем delimiter
     * с кастованием полей
     * @param list
     *            список объектов
     * @param delimiter
     *            разделитель
     * @param postfix
     *            строка для добавления после строки элемента
     * @return строка из элементов списка с добавленным postfix в конце и
     *         разделенных разделителем delimiter
     */
    public static String listToString(Collection<?> list, Map<String, String> castCols,
            String delimiter, String postfix) {
        
        return listToString(listAddPostfix(list, castCols, postfix), delimiter);
    }

    /**
     * Строит список вопросов для передачи экранированых параметров
     * 
     * @param colsList
     *            список колонок
     * @return список вопросов для передачи экранированых параметров
     */
    public static Collection<String> questionMarks(Collection<?> colsList) {
        return questionMarks(colsList, new HashMap<String, String>());
    }

    /**
     * Строит список вопросов для передачи экранированых параметров с заменой
     * значений колонок выражениями из карты подстановки
     * 
     * @param colsList
     *            список колонок
     * @param castCols
     *            карта подстановки вместо колонок
     * @return список вопросов для передачи экранированых параметров
     */
    public static Collection<String> questionMarks(Collection<?> colsList, Map<String, String> castCols) {
        Collection<String> result = new ArrayList<String>();
        for (Object col : colsList) {
            if (castCols.keySet().contains(col)) {
                result.add(castCols.get(col));
            } else {
                result.add(QUESTION);
            }
        }
        return result;
    }
    
    /**
     * Генерирует строку запроса для вставки данных
     * 
     * @param tableName
     *            имя целевой таблицы
     * @param colsList
     *            список колонок
     * @return строка запроса для вставки данных
     */
    public static String constructInsertQuery(String tableName, Collection<String> colsList) {
        return constructInsertQuery(tableName, colsList, new HashMap<String, String>());
    }
    
    /**
     * Генерирует строку запроса для вставки данных с подменой значений колонок 
     * выражениями из карты подстановки
     * 
     * @param tableName
     *            имя целевой таблицы
     * @param colsList
     *            список колонок
     * @param castCols
     *            карта подстановки вместо колонок
     * @return строка запроса для вставки данных
     */
    public static String constructInsertQuery(String tableName, Collection<String> colsList, Map<String, String> castCols) {
        StringBuffer insertQuery = new StringBuffer().append(INSERT_INTO)
                .append(tableName).append("(").append(listToString(colsList, DELIMITER))
                .append(VALUES).append(listToString(questionMarks(colsList, castCols), DELIMITER))
                .append(")");
        
        return insertQuery.toString();
    }
    
    /**
     * Генерирует строку запроса следующего вида: INSERT INTO
     * <table>
     * (<cols>) SELECT (<questionsMarks>) Это позволяет создавать запросы на
     * вставку по условию
     * 
     * @param tableName
     *            имя целевой таблицы
     * @param colsList
     *            список колонок
     * @return строка запроса вставки из запроса выборки
     */
    public static String constructInsertSelectQuery(String tableName, Collection<String> colsList) {
        return constructInsertSelectQuery(tableName, colsList, new HashMap<String, String>()); 
    }

    /**
     * Генерирует строку запроса следующего вида: INSERT INTO
     * <table>
     * (<cols>) SELECT (<questionsMarks>) Это позволяет создавать запросы на
     * вставку по условию
     * с поддержкой кастования полей
     * @param tableName
     *            имя целевой таблицы
     * @param colsList
     *            список колонок
     * @return строка запроса вставки из запроса выборки
     */
    public static String constructInsertSelectQuery(String tableName,
            Collection<String> colsList, Map<String, String> castCols) {
        
        StringBuffer insertQuery = new StringBuffer().append(INSERT_INTO)
                .append(tableName).append("(").append(listToString(colsList, DELIMITER))
                .append(") ").append(constructSelectQuery(questionMarks(colsList, castCols)));
        
        return insertQuery.toString();
    }

    
   
    /**
     * Создает строку запроса на выборку данных
     * 
     * @param colsList
     *            список колонок для вставки
     * @return строкf запроса на выборку данных
     */
    public static String constructSelectQuery(Collection<String> colsList) {
        return constructSelectQuery(colsList, new HashMap<String, String>());
    }
    
    /**
     * Создает строку запроса на выборку данных
     * с кастование полей
     * @param colsList
     *            список колонок для вставки
     * @return строкf запроса на выборку данных
     */
    public static String constructSelectQuery(Collection<String> colsList, Map<String, String> castCols) {
        StringBuffer query = new StringBuffer().append(SELECT).append(
                listToString(colsList, castCols, DELIMITER));

        return query.toString();
    }

    /**
     * Создает строку запроса на выборку данных из таблицы
     * 
     * @param tableName
     *            имя целевой таблицы
     * @param colsList
     *            список колонок
     * @return строка запроса на выборку данных из таблицы
     */
    public static String constructSelectQuery(String tableName, Collection<String> colsList) {
        return constructSelectQuery(tableName, colsList, new HashMap<String, String>());
    }
    
    /**
     * Создает строку запроса на выборку данных из таблицы
     * с кастованием полей
     * @param tableName
     *            имя целевой таблицы
     * @param colsList
     *            список колонок
     * @return строка запроса на выборку данных из таблицы
     */
    public static String constructSelectQuery(String tableName, Collection<String> colsList,
            Map<String, String> castCols) {
        StringBuffer query = new StringBuffer(constructSelectQuery(colsList, castCols)).append(
                FROM).append(tableName);

        return query.toString();
    }

    /**
     * Создает строку запроса на выборку данных из таблицы с условием
     * 
     * @param tableName
     *            имя целевой таблицы
     * @param colsList
     *            список колонок
     * @param whereList
     *            список колонок условия
     * @return строка запроса на выборку данных из таблицы с условием
     */
    public static String constructSelectQuery(String tableName, Collection<String> colsList,
            Collection<String> whereList, String where) {
        return constructSelectQuery(tableName, colsList, new HashMap<String, String>(), whereList, where);
    }
    
    /**
     * Создает строку запроса на выборку данных из таблицы с условием
     * с кастованием полей
     * @param tableName
     *            имя целевой таблицы
     * @param colsList
     *            список колонок
     * @param whereList
     *            список колонок условия
     * @return строка запроса на выборку данных из таблицы с условием
     */
    public static String constructSelectQuery(String tableName, Collection<String> colsList,
            Map<String, String> castCols, Collection<String> whereList, String where) {
        StringBuffer query = new StringBuffer(constructSelectQuery(tableName, colsList, castCols))
                .append(WHERE).append(listToString(whereList, AND, "=?"));
        if (where != null && !where.isEmpty()) {
            query.append(AND).append(where);
        }
        return query.toString();
    }
    
    /**
     * Создает строку запроса на выборку данных из таблицы с условием в группировкой по ключевым полям
     * @param tableName
     * @param colsList
     * @param whereList
     * @param orderByList
     * @return
     */
    public static String constructSelectQuery(String tableName, Collection<String> colsList,
            Collection<String> whereList, Collection<String> orderByList, String where) {
        
        return constructSelectQuery(tableName, colsList, new HashMap<String, String>(), whereList, orderByList, where);
    }
    
    /**
     * Создает строку запроса на выборку данных из таблицы с условием в группировкой по ключевым полям
     * с кастованием полей
     * @param tableName
     * @param colsList
     * @param castCols
     * @param whereList
     * @param orderByList
     * @param where
     * @return
     */
    public static String constructSelectQuery(String tableName, Collection<String> colsList,
            Map<String, String> castCols, Collection<String> whereList, Collection<String> orderByList, String where) {
        
        StringBuffer query = new StringBuffer(constructSelectQuery(tableName, colsList, castCols, whereList, where));
        query.append(ORDER_BY).append(listToString(orderByList, DELIMITER));

        return query.toString();
    }
    
    /**
     * Генерирует строку запроса для обновления данных
     * 
     * @param tableName
     *            имя целевой таблицы
     * @param colsList
     *            список колонок
     * @param whereList
     *            список колонок условия
     * @return строка запроса для обновления данных
     */
    public static String constructUpdateQuery(String tableName, Collection<String> colsList,
            Collection<String> whereList) {
        
        return constructUpdateQuery(tableName, colsList, new HashMap<String, String>(), whereList);
    }
    
    /**
     * Генерирует строку запроса для обновления данных
     * с кастованием полей
     * @param tableName
     *            имя целевой таблицы
     * @param colsList
     *            список колонок
     * @param whereList
     *            список колонок условия
     * @return строка запроса для обновления данных
     */
    public static String constructUpdateQuery(String tableName, Collection<String> colsList,
            Map<String, String> castCols, Collection<String> whereList) {
        
        StringBuffer insertQuery = new StringBuffer().append(UPDATE).append(tableName)
                .append(SET).append(listToString(colsList, castCols, DELIMITER, "=?"))
                .append(WHERE).append(listToString(whereList, AND, "=?"));

        return insertQuery.toString();
    }
    
    /**
     * Создает строку запроса на выборку данных из таблицы с условием
     * 
     * @param tableName
     *            имя целевой таблицы
     * @param colsList
     *            список колонок
     * @param where
     *            условие
     * @return строка запроса на выборку данных из таблицы с условием
     */
    public static String constructSelectQuery(String tableName, Collection<String> colsList, String where) {
        StringBuffer query = new StringBuffer(constructSelectQuery(tableName, colsList))
                .append(WHERE).append(where);

        return query.toString();
    }
    
    /**
     * Создает строку запроса на удаление данных из таблицы с условием
     * 
     * @param tableName
     *            имя целевой таблицы
     * @param whereList
     *            список колонок условия
     * @return
     */
    public static String constructDeleteQuery(String tableName, Collection<String> whereList) {
        StringBuffer query = new StringBuffer().append(DELETE_FROM).append(tableName)
                .append(WHERE).append(listToString(whereList, AND, "=?"));

        return query.toString();
    }
}
