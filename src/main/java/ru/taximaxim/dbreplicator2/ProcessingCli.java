package ru.taximaxim.dbreplicator2;

import java.io.OutputStream;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

public class ProcessingCli extends CommonsCli{
	
	public static final Logger LOG = Logger.getLogger(ProcessingCli.class);

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
	public static void initialization(String[] args) {
		if (args.length != 0) {
			CommonsCli.setOption("h", "help", true, "Print help for this application", 0,
					false, null);
			CommonsCli.setOption("t", "test", true, "The test test test", 1, false, "file");

			CommonsCli.createOptionGroup(new Option("a", true, "A option"));
			CommonsCli.addOptionGroup(new Option("b", true, "B option"));

			//выполение
			CommonsCli.parserCommandLine(args);
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
			printHelp(CommonsCli.getOptions(), // опции по которым составляем help
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
