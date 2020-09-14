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
package ru.taximaxim.dbreplicator2.utils;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * Вспомогательный алгоритмический класс
 *
 * @author ags
 *
 */
public final class Utils {

    /**
     * Данный класс нельзя инстанциировать.
     */
    private Utils() {
    }

    /**
     * Фикс для исправления предупреждения
     * "The expression of type List needs unchecked conversion"
     *
     * @param clazz
     *            Класс к которому нужно кастить список
     * @param c
     *            Коллекция объектов без приведения к нужному типу
     *
     * @return Проверенный список
     */
    public static <T> List<T> castList(Class<? extends T> clazz, Collection<?> c) {
        List<T> r = new ArrayList<>(c.size());
        for (Object o : c) {
            r.add(clazz.cast(o));
        }
        return r;
    }

    /**
     * Превращает строку с параметрами в объект настроек
     *
     * @param params
     *               Строка с параметрами
     * @return Заполненные настройки
     */
    public static Properties convertParamStringToProperties(String param) {
        Properties properties = new Properties();
        if (param != null) {
            try {
                properties.load(new StringReader(param));
            } catch (IOException e) {
                Logger.getLogger(Utils.class)
                .error("Ошибка при чтение параметров [" + param + "]!", e);
            }
        }

        return properties;
    }

    /**
     * Превращает настройки в строку с параметрами
     *
     * @param properties
     *               Настройки
     * @return Строка с параметрами
     */
    public static String convertPropertiesToParamString(Properties properties) {
        StringWriter writer = new StringWriter();
        properties.list(new PrintWriter(writer));
        return writer.getBuffer().toString();
    }
}
