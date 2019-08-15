#! /bin/bash

DIR=`dirname $0`

cd $DIR/..
DIR=`pwd`

CLASSPATH=$CLASSPATH:$DIR/conf:$DIR$DIR/data

for i in $DIR/lib/*.jar; do
    CLASSPATH=$CLASSPATH:$i;
done

MAINCLASS=org.apache.hadoop.contrib.ftp.HdfsOverFtpServer
XMX=8g
XMS=8g

LOGDIR=$DIR/logs
mkdir -p $LOGDIR

PIDFILE=$LOGDIR/hdfs-over-ftp.pid
OUTFILE=$LOGDIR/hdfs-over-ftp.out
#OPT="-Dport=19998 -Dservers=h1:9092,h2:9092,h3:9092 -Dtopic=oneTopic"
start(){
        echo "start hdfs-over-ftp server..."

        if [ -e $PIDFILE ]; then
                pid=`cat $PIDFILE`
                find=`ps -ef | grep $pid | grep -v grep | awk '{ print $2 }'`
                if [ -n "$find" ]; then
                        echo "hdfs-over-ftp server has already been start."
                        exit
                fi
        fi

        nohup $JAVA_HOME/bin/java -cp $CLASSPATH -Xmx${XMX} -Xms${XMS} $MAINCLASS </dev/null 2>&1 >> $OUTFILE &
        echo $! > $PIDFILE
        if [ -e $PIDFILE ]; then
        pid=`cat $PIDFILE`
        find=`ps -ef | grep $pid | grep -v grep | awk '{ print $2 }'`
        if [ -n "$find" ]; then
            echo "hdfs-over-ftp is stared"
            exit
        fi
    fi

    echo "start hdfs-over-ftp fail"
}

stop() {
        echo "stop hdfs-over-ftp server..."

        if [ -e $PIDFILE ]; then
                pid=`cat $PIDFILE`
                find=`ps -ef | grep $pid | grep -v grep | awk '{ print $2 }'`
                if [ -n "$find" ]; then
                        kill -9 $find

                        while [ -n "`ps -ef | grep $pid | grep -v grep | awk '{ print $2 }'`" ]; do
                                sleep 1;
                        done
                        echo "ok"
                        exit
                fi
        fi
        echo "no hdfs-over-ftp server found"
}

case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
        *)
        echo "Usage: hdfs-over-ftp-run.sh {start|stop}"
esac
