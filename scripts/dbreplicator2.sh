#!/bin/bash

DBREP_HOME=$(cd "`dirname $0`/.." && pwd)

export LANG=ru_RU.UTF-8
export LANGUAGE=ru
export LC_CTYPE=ru_RU.UTF-8

export TZ=UTC

logfile=/dev/stdout

pidfile=$DBREP_HOME/run/dbreplicator2.pid

quit() {
    local ex=$?
    echo -e "${1}"
    rm $pidfile &>/dev/null
    exit ${ex}
}

# Обработка выхода. -> dbreplicator2 stop
if [ x"$1" == x"stop" ]; then
    echo -n Stopping dbreplicator2... 
    if [ -f $pidfile ]; then
        read pid < $pidfile
        if ps $pid &>/dev/null ; then
            echo Process $pid found. Sending SIGTERM 
            kill $pid && quit "Process dbreplicator2 terminated success." 
        fi
    else
        echo " Error: pid file ($pidfile) not found. Please, stop dbreplicator2 manually!"
    fi
    exit
fi

test -f $pidfile && {
    read pid < $pidfile

    ps $pid &>/dev/null && 
        die "$cmd is already running... exiting" || 
        die "$cmd is dead but pidfile exists... exiting" 
    }


echo -n "$$" > $pidfile


classdir=$DBREP_HOME/lib/

jcmd="java -Xmx512m -cp ""${classdir}dbreplicator2.jar:${classdir}log4j-1.2.17.jar:${classdir}mail-1.4.jar:${classdir}commons-cli-1.2.jar:${classdir}hibernate-core-4.2.6.Final.jar:${classdir}hibernate-commons-annotations-4.0.2.Final.jar:${classdir}hibernate-jpa-2.0-api-1.0.1.Final.jar:${classdir}h2-1.3.173.jar:${classdir}bonecp-0.7.1.RELEASE.jar:${classdir}dom4j-1.6.1.jar:${classdir}jboss-logging-3.1.0.GA.jar:${classdir}jboss-transaction-api_1.1_spec-1.0.1.Final.jar:${classdir}javassist-3.15.0-GA.jar:${classdir}antlr-2.7.7.jar:${classdir}slf4j-log4j12-1.5.5.jar:${classdir}slf4j-api-1.5.10.jar:${classdir}guava-r08.jar:${DBREP_HOME}/resources/"""

exec ${jcmd} ru.taximaxim.dbreplicator2.Application "$1" "$2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" "${10}" "${11}" >> $logfile 2>&1

rm $pidfile
exit 0
