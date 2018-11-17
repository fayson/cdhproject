# HBASE_CLASSPASTH={{HBASE_CLASSPATH}}
# JAVA_LIBRARY_PATH={{JAVA_LIBRARY_PATH}}
export HBASE_CLASSPATH=`echo $HBASE_CLASSPATH | sed -e "s|$ZOOKEEPER_CONF:||"`
export HBASE_OPTS="-Xmx268435456 -XX:+HeapDumpOnOutOfMemoryError -XX:+UseConcMarkSweepGC -XX:+CMSIncrementalMode -Djava.net.preferIPv4Stack=true $HBASE_OPTS"
