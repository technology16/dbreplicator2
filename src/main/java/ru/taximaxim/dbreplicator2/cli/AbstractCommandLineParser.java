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

package ru.taximaxim.dbreplicator2.cli;

import org.apache.commons.cli.AlreadySelectedException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

public abstract class AbstractCommandLineParser {

	private Options posixOptions = new Options();
	private OptionGroup optionGroup = new OptionGroup();
	
	private static final Logger LOG = Logger.getLogger(AbstractCommandLineParser.class);
	
	/**
	 * получение опций
	 * 
	 * @return
	 */
	protected Options getOptions() {
		return posixOptions;
	}

	/**
	 * Установка опций
	 * 
	 * @param opt
	 *            - сокращенное имя вызова опций
	 * 
	 * @param longOpt
	 *            - полное имя вызова опций
	 * 
	 * @param hasArg
	 *            - наличие аргумента
	 * 
	 * @param description
	 *            - Описание
	 * 
	 * @param num
	 *            <code>Integer</code> - Номер аргумента
	 * 
	 * @param optionalArg
	 *            - Дополнительный аргумент <code>boolean</code>
	 * 
	 * @param argName
	 *            - Имя аргумента
	 * 
	 */
	protected void setOption(String opt, String longOpt, boolean hasArg,
			String description, Integer num, boolean optionalArg, String argName) {
		Option option = new Option(opt, longOpt, hasArg, description);

		if (num != null) {
			option.setArgs(num);
		}
		option.setOptionalArg(optionalArg);
		option.setArgName(argName);
		posixOptions.addOption(option);
	}

	/**
	 * Создание новых групп опций Используеться при создание новых опций
	 * 
	 * @param option
	 *            - опции
	 */
	protected void createOptionGroup(Option option) {
		processingOptionGroup(option, false, true);
	}

	/**
	 * Установка групп опций Использование при записи опций групп Не добавляет
	 * опций после того как установите послению опцию
	 * <code>addOptionGroup(Option option)</code>
	 * 
	 * @param option
	 */
	protected void setOptionGroup(Option option) {
		processingOptionGroup(option, false, false);
	}

	/**
	 * Добавление групп опций в опции Для создания первой опций используйте
	 * <code>createOptionGroup(Option option)</code>
	 * 
	 * @param option
	 *            - опции
	 */
	protected void addOptionGroup(Option option) {
		processingOptionGroup(option, true, false);
	}

	/**
	 * Обработка груп опций
	 * 
	 * @param option
	 *            - опции
	 * 
	 * @param add
	 *            - добавить в группу опцию
	 * 
	 * @param clear
	 *            - очистить группу опцию
	 */
	private void processingOptionGroup(Option option, boolean add, boolean clear) {
		if (clear == true) {
			optionGroup = new OptionGroup();
		}

		optionGroup.addOption(option);

		if (add) {
			posixOptions.addOptionGroup(optionGroup);
		}
	}

	/**
	 * parser command line
	 * 
	 * @param args
	 */
	protected void parserCommandLine(String[] args) {

		CommandLineParser cmdLinePosixParser = new PosixParser();
		CommandLine commandLine = null;
		try {
			
			commandLine = cmdLinePosixParser.parse(getOptions(), args);
			processingCmd(commandLine);
			
		} catch (AlreadySelectedException ex) {
			LOG.error(String.format("Ошибка опций групп: %s", ex.getMessage()), ex);
		} catch (ParseException ex) {
			LOG.error("Неправильный синтаксис команд", ex);
		}
	}
	
	protected abstract void processingCmd(CommandLine commandLine);
	
}
