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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.log4j.Logger;

import ru.taximaxim.dbreplicator2.Application;

public class CommandLineParser extends AbstractCommandLineParser {

    private static final Logger LOG = Logger.getLogger(CommandLineParser.class);

    public static CommandLineParser parse(String[] args) {
        return new CommandLineParser(args);
    }

    /**
     * Обработка
     *
     * @param args
     *            <code>String[]</code> - аргументы командной строки
     */
    protected CommandLineParser(String[] args) {
        if (args.length != 0) {
            setOption("h", "help", true, "Справка", 0,
                    false, null);
//            setOption("t", "test", true, "The test test test", 1, false, "file");
            setOption("i", "init", false, "Инициализация настроек dbreplicator2", 0, false, null);
            setOption("u", "update", true, "Обновление настроек dbreplicator2 скриптом из файла ", 1, false, "file");
            setOption("s", "start", false, "Запуск репликации dbreplicator2", 0, false, null);

//            createOptionGroup(new Option("a", true, "A option"));
//            addOptionGroup(new Option("b", true, "B option"));

            // выполение
            parserCommandLine(args);
        }
    }

    /**
     * Обработка команд
     *
     * @param commandLine
     */
    protected void processingCmd(CommandLine commandLine) {
        boolean hasOption = false;

/*        if (commandLine.hasOption("t")) {
            String[] arguments = commandLine.getOptionValues("t");
            LOG.debug("Name of the test: " + arguments[0]);
            error = false;
        }*/

        if (commandLine.hasOption("i")) {
            // Инициализируем БД настроек
            hasOption = true;
        }

        if (commandLine.hasOption("u")) {
            // Обновляем БД настроек скриптом из файла
            hasOption = true;
        }

        if (commandLine.hasOption("s")) {
            // Запускаем репликацию
            // Определение ведущих БД и запуск процессов диспетчеров записей
            // для каждой ведущей БД.
            // 1. Определяем ведущие БД по существующим настройкам.
            // 2. Запуск диспечеров записей для каждой ведущей БД.
            Application.getTasksPool().start();
            hasOption = true;
        }

        if (commandLine.hasOption("h") || !hasOption) {
            if (!hasOption) {
                LOG.error("Неизвестная команда, пожалуйста воспользуетесь командой [-h] или [--help]");
            }

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java dbreplicator2.jar", getOptions());
        }
    }
}
