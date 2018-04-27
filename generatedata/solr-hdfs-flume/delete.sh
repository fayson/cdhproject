#!/bin/sh

ZK="ip-172-31-5-171.ap-southeast-1.compute.internal"
COLLECTION="collection1"
BASE=`pwd`

echo "delete solr collection"
solrctl --zk $ZK:2181/solr collection --delete $COLLECTION
solrctl --zk $ZK:2181/solr instancedir --delete $COLLECTION
# sudo -u hdfs hdfs dfs -rm -r -skipTrash /solr/$COLLECTION
rm -rf tmp/*
