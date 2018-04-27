INPUT='/fayson/solr'
COLLECTION='collection1'
NN='ip-172-31-8-230.ap-southeast-1.compute.internal'
ZK='ip-172-31-8-230.ap-southeast-1.compute.internal'

echo 'Delete previous docs...'
solrctl collection --deletedocs $COLLECTION

echo 'MapReduce indexing...'
config="/etc/hadoop/conf.cloudera.yarn"
parcel="/opt/cloudera/parcels/CDH"
jar="$parcel/lib/solr/contrib/mr/search-mr-*-job.jar"
libjars="lib/mtools-0.4.0.jar"
cls="org.apache.solr.hadoop.MapReduceIndexerTool"
opts="'mapred.child.java.opts=-Xmx1024m'"
log4j="$parcel/share/doc/search*/examples/solr-nrt/log4j.properties"
input="hdfs://$NN:8020/$INPUT"
output="hdfs://$NN:8020/tmp/$COLLECTION"
zk="$ZK:2181/solr"

hdfs dfs -rm -r -f -skipTrash $output
hadoop --config $config jar $jar $cls --libjars $libjars -D $opts --log4j $log4j --morphline-file conf/morphlines.conf --output-dir $output --verbose --go-live --zk-host $zk --collection $COLLECTION $input
