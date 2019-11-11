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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;
import org.hibernate.HibernateException;
import org.hibernate.SQLQuery;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
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
    protected void processingCmd(CommandLine commandLine) throws Exception {
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

        // Если имя файла настроек лога оканчивается на .xml, то используем
        // DOMConfigurator
        if (fLog4j.toLowerCase().endsWith(".xml")) {
            DOMConfigurator.configureAndWatch(fLog4j);
        } else {
            // иначе используем PropertyConfigurator
            PropertyConfigurator.configureAndWatch(fLog4j);
        }

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

        String hibernateHbm2ddlImportFiles = null;
        if (commandLine.hasOption('u')) {
            String[] arguments = commandLine.getOptionValues('u');
            hibernateHbm2ddlImportFiles = arguments[0];
            hasOption = true;
        }

        boolean coreGetTasksPoolStart = false;
        if (commandLine.hasOption('s')) {
            // Запускаем репликацию
            // Определение ведущих БД и запуск процессов диспетчеров записей
            // для каждой ведущей БД.
            // 1. Определяем ведущие БД по существующим настройкам.
            // 2. Запуск диспечеров записей для каждой ведущей БД.
            coreGetTasksPoolStart = true;
            hasOption = true;
        }

        if (commandLine.hasOption('h') || !hasOption) {
            if (!hasOption) {
                LOG.error(
                        "Неизвестная команда, пожалуйста воспользуетесь командой [-h] или [--help]");
            }

            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("java dbreplicator2.jar", getOptions());
        }

        if (hasOption) {
            if (hibernateHbm2ddlAuto && hibernateHbm2ddlImportFiles != null) {
                start(configurationName, hibernateHbm2ddlAuto,
                        hibernateHbm2ddlImportFiles, coreGetTasksPoolStart);
            } else if (coreGetTasksPoolStart) {
                start(configurationName, hibernateHbm2ddlAuto,
                        hibernateHbm2ddlImportFiles, coreGetTasksPoolStart);
            } else if (hibernateHbm2ddlImportFiles != null) {
                start(configurationName, hibernateHbm2ddlAuto,
                        hibernateHbm2ddlImportFiles, coreGetTasksPoolStart);
            }
        }
    }

    /**
     * Обновляем БД настроек скриптом из файла
     * 
     * @param importFiles
     * @param configuration
     * @throws Exception 
     */
    protected void importFiles(String importFiles, SessionFactory sessionFactory) throws Exception {
        Session session = sessionFactory.openSession();

        Transaction tx = null;
        try {
            tx = session.beginTransaction();

            for ( String currentFile : importFiles.split(",") ) {
                Path path = Paths.get(Application.class.getClassLoader().getResource(currentFile).toURI());
                SQLQuery query = session.createSQLQuery(
                        new String(
                                Files.readAllBytes(path), 
                                "UTF-8"));
                query.executeUpdate();
            }
            tx.commit();
        } catch (HibernateException | URISyntaxException | IOException e) {
            if (null != tx) {
                tx.rollback();
            }
            
            throw e;
        } finally {
            session.close();
        }
    }

    protected void start(String configurationName, boolean hibernateHbm2ddlAuto,
            String hibernateHbm2ddlImportFiles, boolean coreGetTasksPoolStart) throws Exception {
        // Конфигурируем Hibernate
        Configuration configuration;
        // Инициализируем БД настроек
        configuration = Core.getConfiguration(configurationName);

        if (hibernateHbm2ddlAuto) {
            // Инициализируем БД настроек
            configuration.setProperty("hibernate.hbm2ddl.auto", "create");
        }

        SessionFactory sessionFactory = Core.getSessionFactory(configuration);

        if (hibernateHbm2ddlImportFiles != null) {
            importFiles(hibernateHbm2ddlImportFiles, sessionFactory);
        }

        if (coreGetTasksPoolStart) {
            Core.getTasksPool().start();
            Core.getCronPool().start();
        }
    }

    /**
     * Точка входа
     * 
     * @param args
     * @throws Exception 
     */
    public static void main(String[] args) throws Exception {
        new Application().parserCommandLine(args);
    }

}
