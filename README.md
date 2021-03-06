dbreplicator2
=============

Набор утилит для репликации данных между гетерогенными базами данных используя JDBC

## Базовые принципы

1. Репликатор выполняет репликацию модифицированных записей, а не операций.

2. Репликация записей осуществляется независимым потоком, который ограничен набором обслуживаемых таблиц и целевой базы данных.

3. В случае возникновения ошибки во время выполнения потока, поток приостанавливает свою работу на определенное время, после которого пытается повторно выполнить операцию. Ошибка в одном потоке, не отражается на работоспособность других потоков.

4. Поток должен иметь настраиваемую стратегию переноса записей (последовательность переноса по таблицам, необходимость остановки в случае возникновения ошибки), а также возможность применения последовательно разных стратегий в случае ошибки переноса по одной из существующих.

## Компоненты:

![](docs/images/components.png?raw=true "Укрупненная схема архитектуры приложения")

1. Факт того, что была изменена запись реплицируемой таблицы отражается в супер лог таблице.

2. На основании данных из супер лог таблицы менеджер записей генерирует операции в таблице записей очереди репликации для целевых очередей репликации и помечает их как ожидающих обработки. Исходные записи из супер лог таблицы удаляются.

3. Обработчик очереди репликации начинает транзакцию, извлекает записи помеченные как ожидающие обработки, помечает их как данные в обработке и строит список измененных записей.

4. Производится вставка/изменение/удаление измененных записей в целевой БД.

5. Если данные перенесены корректно, то удаляются исходные данные из очереди репликации, иначе исходные записи помечаются как ожидающие обработки. Транзакция закрывается.

## Жизненный цикл работы репликтора

1. Старт приложения, чтение настроек о зарегистрированных пулах соединений.

2. Инициализация именованных пулов соединений.

3. Определение рабочих потоков, подготовка пула потоков

4. Определение ведущих БД и запуск процессов диспетчеров для каждой ведущей БД.

 1. По мере заполнения суперлога, процесс диспетчер выполняет копирование данных по пулам
рабочих потоков, после копирования данных диспетчер запускает рабочий поток для каждого пула
в который были скопированы данные.

 2. Рабочий поток лочит данные доступные для репликации и выполняет репликацию данных.
TODO: На данном этапе данные находятся еще в режиме операций - нужно определить как выполнять 
трансформацию операций, в модифицированные данные.


