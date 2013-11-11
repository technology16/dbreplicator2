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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.hibernate.cfg.Configuration;

import ru.taximaxim.dbreplicator2.cli.AbstractCommandLineParser;
import ru.taximaxim.dbreplicator2.utils.Core;

/**
 * @author TaxiMaxim
 * 
 */
public final class Application extends AbstractCommandLineParser {

    private static final Logger LOG = Logger.getLogger(Core.class);

    /**
     * Данный класс нельзя инстанциировать.
     */
    private Application() {
        setOption("h", "help", true, "Справка", 0, false, null);
        setOption("i", "init", false, "Инициализация настроек dbreplicator2", 0, false,
                null);
        setOption("u", "update", true,
                "Обновление настроек dbreplicator2 скриптом из файла", 1, false, "file");
        setOption("c", "conf", true,
                "Явное задание пути к файлу конфигурации. По умолчанию берется файл hibernate.cfg.xml", 
                1, false, "file");
        setOption("l", "log4j", true,
                "Явное задание пути к файлу настроек логгирования. По умолчанию берется файл log4j.properties", 
                1, false, "file");
        setOption("s", "start", false, "Запуск репликации dbreplicator2", 0, false, null);
    }

    @Override
    protected void processingCmd(CommandLine commandLine) {
        boolean hasOption = false;

        LOG.info("Запускаем dbreplicator2...");
        
        // Конфигурируем log4j
        String fLog4j = "log4j.properties";
        if (commandLine.hasOption('l')) {
            String[] arguments = commandLine.getOptionValues('l');
            // Инициализируем БД настроек
            fLog4j = arguments[0];

            hasOption = true;
        }
        PropertyConfigurator.configureAndWatch(fLog4j);

        
        String configurationName = null;
        if (commandLine.hasOption('c')) {
            String[] arguments = commandLine.getOptionValues('c');
            configurationName = arguments[0];
            hasOption = true;
        }

        boolean hibernateHbm2ddlAuto = false;
        if (commandLine.hasOption('i')) {
            hibernateHbm2ddlAuto = true;
            hasOption = true;
        }

        String hibernate_hbm2ddl_import_files = null;
        if (commandLine.hasOption('u')) {
            String[] arguments = commandLine.getOptionValues('u');
            hibernate_hbm2ddl_import_files = arguments[0];
            hasOption = true;
        }
        
        boolean CoreGetTasksPoolStart = false;
        if (commandLine.hasOption('s')) {
            // Запускаем репликацию
            // Определение ведущих БД и запуск процессов диспетчеров записей
            // для каждой ведущей БД.
            // 1. Определяем ведущие БД по существующим настройкам.
            // 2. Запуск диспечеров записей для каждой ведущей БД.
            CoreGetTasksPoolStart = true;
            hasOption = true;
        }

        if (commandLine.hasOption('h') || !hasOption) {
            if (!hasOption) {
                LOG.error("Неизвестная команда, пожалуйста воспользуетесь командой [-h] или [--help]");
            } else {
                isValidate(configurationName, hibernateHbm2ddlAuto, 
                        hibernate_hbm2ddl_import_files, CoreGetTasksPoolStart);
            }

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java dbreplicator2.jar", getOptions());
        }
    }

    protected void isValidate(String configurationName, boolean hibernateHbm2ddlAuto
            ,String hibernate_hbm2ddl_import_files, boolean CoreGetTasksPoolStart) {
        // Конфигурируем Hibernate
        Configuration configuration;
        // Инициализируем БД настроек
        configuration = Core.getConfiguration(configurationName);

        if(hibernateHbm2ddlAuto) {
            // Инициализируем БД настроек
            configuration.setProperty("hibernate.hbm2ddl.auto", "create");
        }

        if(hibernate_hbm2ddl_import_files!=null) {
            // Обновляем БД настроек скриптом из файла
            configuration.setProperty("hibernate.hbm2ddl.import_files", hibernate_hbm2ddl_import_files);
        }
        Core.getSessionFactory(configuration);
        
        if(CoreGetTasksPoolStart) {
            Core.getTasksPool().start();
        }
        
    }
    
    public static void main(String[] args) {
        new Application().parserCommandLine(args);
    }

}
