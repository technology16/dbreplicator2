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

package ru.taximaxim.dbreplicator2;

import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.log4j.Logger;

public class ProcessingCli extends CommonsCli{
	
	public static final Logger LOG = Logger.getLogger(ProcessingCli.class);

	/**
	 * Вывод помошника
	 */
	private static void printHelp(final Options options,
			final int printedRowWidth, final String header,
			final String footer, final int spacesBeforeOption,
			final int spacesBeforeOptionDescription,
			final boolean displayUsage, final OutputStream out) {
		final String commandLineSyntax = "java dbreplicator2.jar";
		final PrintWriter writer = new PrintWriter(out);
		final HelpFormatter helpFormatter = new HelpFormatter();

		helpFormatter.printHelp(writer, printedRowWidth, commandLineSyntax,
				header, options, spacesBeforeOption,
				spacesBeforeOptionDescription, footer, displayUsage);
		writer.flush();
	}	
	
	/**
	 * Обработка
	 * 
	 * @param args <code>String[]</code>
	 *            - аргументы командной строки
	 */
	public static void initialize(String[] args) {
		if (args.length != 0) {
			ProcessingCli.setOption("h", "help", true, "Print help for this application", 0,
					false, null);
			ProcessingCli.setOption("t", "test", true, "The test test test", 1, false, "file");

			ProcessingCli.createOptionGroup(new Option("a", true, "A option"));
			ProcessingCli.addOptionGroup(new Option("b", true, "B option"));

			//выполение
			ProcessingCli.parserCommandLine(args);
		}
	}

	/**
	 * Обработка команд
	 * 
	 * @param commandLine
	 */
	protected static void processingCmd(CommandLine commandLine) {

		boolean error = true;
		if (commandLine.hasOption("t")) {
			String[] arguments = commandLine.getOptionValues("t");
			System.out.println("Name of the test: " + arguments[0]);
			error = false;
		}

		if (commandLine.hasOption("h")) {
			printHelp(ProcessingCli.getOptions(), // опции по которым составляем help
					80, // ширина строки вывода
					"Options:", // строка предшествующая выводу
					"-- HELP --", // строка следующая за выводом
					3, // число пробелов перед выводом опции
					5, // число пробелов перед выводом опцисания опции
					true, // выводить ли в строке usage список команд
					System.out // куда производить вывод
			);
			error = false;
		}
		
		if (error) {
			LOG.error("Неизвестная команда, пожалуйста воспользуетесь командой [-h] или [--help]");
		}
	}
}
