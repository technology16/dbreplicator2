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
    private QueryConstructors() {

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
        StringBuffer result = new StringBuffer();

        boolean setComma = false;
        for (Object val : list) {
            if (setComma) {
                result.append(delimiter);
            } else {
                setComma = true;
            }
            result.append(val).append(postfix);
        }

        return result.toString();
    }

    /**
     * Строит список вопросов для передачи экранированых параметров
     * 
     * @param colsList
     *            список колонок
     * @return список вопросов для передачи экранированых параметров
     */
    public static Collection<String> questionMarks(Collection<?> colsList) {
        Collection<String> result = new ArrayList<String>();
        int colsListSize = colsList.size();
        for (int i = 0; i < colsListSize; i++) {
            result.add(QUESTION);
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
        StringBuffer insertQuery = new StringBuffer().append(INSERT_INTO)
                .append(tableName).append("(").append(listToString(colsList, DELIMITER))
                .append(VALUES).append(listToString(questionMarks(colsList), DELIMITER))
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
    public static String constructInsertSelectQuery(String tableName,
            Collection<String> colsList) {
        StringBuffer insertQuery = new StringBuffer().append(INSERT_INTO)
                .append(tableName).append("(").append(listToString(colsList, DELIMITER))
                .append(") ").append(constructSelectQuery(questionMarks(colsList)));

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
        StringBuffer query = new StringBuffer().append(SELECT).append(
                listToString(colsList, DELIMITER));

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
        StringBuffer query = new StringBuffer(constructSelectQuery(colsList)).append(
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
        StringBuffer query = new StringBuffer(constructSelectQuery(tableName, colsList))
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
        StringBuffer query = new StringBuffer(constructSelectQuery(tableName, colsList, whereList, where));
        query.append(ORDER_BY).append(listToString(orderByList, DELIMITER));

        return query.toString();
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
    public static String constructSelectQuery(String tableName, Collection<String> colsList,
            String where) {
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
        StringBuffer insertQuery = new StringBuffer().append(UPDATE).append(tableName)
                .append(SET).append(listToString(colsList, DELIMITER, "=?"))
                .append(WHERE).append(listToString(whereList, AND, "=?"));

        return insertQuery.toString();
    }
    
    /**
     * Генерирует строку запроса для вставки данных с подменой значений колонок 
     * выражениями из карты подстановки
     * 
     * @param tableName
     *            имя целевой таблицы
     * @param colsList
     *            список колонок
     * @param castColumns
     *            карта подстановки вместо колонок
     * @return строка запроса для вставки данных
     */
    public static String constructInsertQuery(String tableName, Collection<String> colsList, Map<String, String> castColumns) {
        StringBuffer insertQuery = new StringBuffer().append(INSERT_INTO)
                .append(tableName).append("(").append(listToString(colsList, DELIMITER))
                .append(VALUES).append(listToString(questionMarks(colsList, castColumns), DELIMITER))
                .append(")");
        
        return insertQuery.toString();
    }

    /**
     * Строит список вопросов для передачи экранированых параметров с заменой
     * значений колонок выражениями из карты подстановки
     * 
     * @param colsList
     *            список колонок
     * @param castColumns
     *            карта подстановки вместо колонок
     * @return список вопросов для передачи экранированых параметров
     */
    public static Collection<String> questionMarks(Collection<?> colsList, Map<String, String> castColumns) {
        Collection<String> result = new ArrayList<String>();
        for (Object col : colsList) {
            if (castColumns.keySet().contains(col)) {
                result.add(castColumns.get(col));
            } else {
                result.add(QUESTION);
            }
        }

        return result;
    }
    
    /**
     * Строит строку из элементов списка, разделенных разделителем delimiter
     * с заменой значений колонок выражениями из карты подстановки
     * 
     * @param list
     *            список объектов
     * @param castColumns
     *            карта подстановки вместо колонок
     * @param delimiter
     *            разделитель
     * @return строка из элементов списка, разделенных разделителем delimiter
     */
    public static String listToString(Collection<?> list, Map<String, String> castColumns, String delimiter) {
        StringBuffer result = new StringBuffer();

        boolean setComma = false;
        for (Object val : list) {
            if (setComma) {
                result.append(delimiter);
            } else {
                setComma = true;
            }
            if (castColumns.keySet().contains(val)) {
                result.append(castColumns.get(val));
            } else {
                result.append(val);
            }
        }

        return result.toString();
    }
}
