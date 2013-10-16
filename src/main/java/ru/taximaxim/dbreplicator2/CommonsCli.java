package ru.taximaxim.dbreplicator2;

import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.log4j.Logger;

public class CommonsCli {

	static Options posixOptions = new Options();
	static OptionGroup optionGroup = new OptionGroup();
	public static final Logger LOG = Logger.getLogger(CommonsCli.class);
	
	/**
	 * Установка опций
	 * 
	 * @param opt
	 * @param longOpt
	 * @param hasArg
	 * @param description
	 * @param num
	 * @param optionalArg
	 * @param argName
	 */
	private static void setOption(String opt, String longOpt, boolean hasArg,
			String description, Integer num, boolean optionalArg, String argName) {
		setOption(opt, longOpt, hasArg, description, num, optionalArg, argName,
				true);
	}

	/**
	 * Установка опций
	 * @param opt
	 * @param longOpt
	 * @param hasArg
	 * @param description
	 * @param num
	 * @param optionalArg
	 * @param argName
	 * @param add
	 *            - запись опций
	 */
	private static void setOption(String opt, String longOpt, boolean hasArg,
			String description, Integer num, boolean optionalArg,
			String argName, boolean add) {
		Option option = new Option(opt, longOpt, hasArg, description);

		option.setArgs(num);
		option.setOptionalArg(optionalArg);
		option.setArgName(argName);

		if (add) {
			posixOptions.addOption(option);
		}
	}

	/**
	 * установка в группу опций
	 * 
	 * @param option
	 */
	private static void setOptionGroup(Option option) {
		setOptionGroup(option, true, false);
	}
	
	/**
	 * установка груп опций
	 * 
	 * @param option
	 * @param add
	 *            - добавить опцию в группу
	 */
	private static void setOptionGroup(Option option, boolean add) {
		setOptionGroup(option, add, false);
	}
	
	/**
	 * установка груп опций
	 * 
	 * @param option
	 * @param add
	 *            - добавить в группу опцию
	 * @param clear
	 *            - очистить группу опцию
	 */
	private static void setOptionGroup(Option option, boolean add, boolean clear) {
		if (clear == true) {
			optionGroup = new OptionGroup();
		}

		optionGroup.addOption(option);

		if (add) {
			posixOptions.addOptionGroup(optionGroup);
		}
	}

	/**
	 * Обработка команд
	 * 
	 * @param commandLine
	 */
	private static void processingCmd(CommandLine commandLine) {

		boolean error = true;
		if (commandLine.hasOption("t")) {
			String[] arguments = commandLine.getOptionValues("t");
			System.out.println("Name of the test: " + arguments[0]);
			error = false;
		}

		if (commandLine.hasOption("h")) {
			printHelp(posixOptions, // опции по которым составляем help
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

	/**
	 * Вывод помошника
	 * 
	 * @param options
	 * @param printedRowWidth
	 * @param header
	 * @param footer
	 * @param spacesBeforeOption
	 * @param spacesBeforeOptionDescription
	 * @param displayUsage
	 * @param out
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
	 * @param args
	 *            - аргументы
	 * @throws ParseException
	 */
	public static void initialization(String[] args) throws ParseException {
		if (args.length != 0) {
			setOption("h", "help", false, "Print help for this application", 0,
					false, null);
			setOption("t", "test", true, "The test test test", 1, false,
					"test name");

			CommandLineParser cmdLinePosixParser = new PosixParser();
			CommandLine commandLine = cmdLinePosixParser.parse(posixOptions,
					args);

			processingCmd(commandLine);
		}
	}
}
