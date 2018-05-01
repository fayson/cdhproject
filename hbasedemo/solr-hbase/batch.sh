COLLECTION='collection1'
ZK='ip-172-31-5-38.ap-southeast-1.compute.internal'

echo 'Delete previous docs...'
solrctl collection --deletedocs $COLLECTION

echo 'Lily HBase MapReduce indexing...'
config="/etc/hadoop/conf.cloudera.yarn"
parcel="/opt/cloudera/parcels/CDH"
jar="$parcel/lib/hbase-solr/tools/hbase-indexer-mr-*-job.jar"
hbase_conf="/etc/hbase/conf/hbase-site.xml"
opts="'mapred.child.java.opts=-Xmx1024m'"
log4j="$parcel/share/doc/search*/examples/solr-nrt/log4j.properties"
zk="$ZK:2181/solr"
libjars="lib/lucene-analyzers-smartcn-4.10.3-cdh5.14.2.jar"

export HADOOP_OPTS="-Djava.security.auth.login.config=conf/jaas.conf" 
hadoop --config $config jar $jar --conf $hbase_conf --libjars $libjars -D $opts --log4j $log4j --hbase-indexer-file conf/indexer-config.xml --verbose --go-live --zk-host $zk --collection $COLLECTION

