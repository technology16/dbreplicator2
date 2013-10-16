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

import java.util.HashMap;
import java.util.Map;

/**
 * Сохраняет все настройки в памяти, для использования с реализацией персистентного
 * хранилища должен реализовать методы load() и save()
 * 
 * @author ags
 *
 */
public abstract class AbstractSettings implements Settings {
	
	protected Map<String, String> settings = new HashMap<String, String>();
	
	protected boolean settingsChanged;

	public String getValue(String key) {
		return settings.get(key);
	}

	public String getValue(String key, String defaultValue) {
		
		String retval = getValue(key);
		
		return retval != null ? retval : defaultValue;
	}

	public void setValue(String key, String value) {
		
		settings.put(key, value);
		settingsChanged = true;
	}

	public boolean isDirty() throws ConfigurableError {
		return settingsChanged;
	}
}
