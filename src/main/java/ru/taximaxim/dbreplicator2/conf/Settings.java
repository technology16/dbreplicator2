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

package ru.taximaxim.dbreplicator2.conf;

/**
 * Базовый интрефейс для работы с настройками приложения.
 * 
 * @author ags
 *
 */
public interface Settings extends Configurable {

	/**
	 * Возвращает значение по ключу
	 * 
	 * @param key ключ для поиска значения
	 * 
	 * @return значение по ключу, возвращает null в случае, если значение
	 * не обнаружено.
	 */
	public String getValue(String key);

	/**
	 * Возвращает значение по ключу
	 * 
	 * @param key ключ для поиска значения
	 * @param defaultValue значение по умолчанию, в случае, если значение 
	 * не обнаружено
	 * 
	 * @return значение по ключу, возвращает defaultValue в случае, если значение
	 * не обнаружено.
	 */
	public String getValue(String key, String defaultValue);
	
	/**
	 * Устанавливает значение ключа 
	 * 
	 * @param key Ключ
	 * @param value Текущее значение. В случае, если null то ключ может быть 
	 * удален из хранилища значений.
	 *  
	 */
	public void setValue(String key, String value);
	
}


