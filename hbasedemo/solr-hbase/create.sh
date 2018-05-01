#!/bin/sh

ZK="ip-172-31-5-171.ap-southeast-1.compute.internal"
COLLECTION="collection1"
BASE=`pwd`
SHARD=3
REPLICA=1

echo "create solr collection"
rm -rf tmp/*
solrctl --zk $ZK:2181/solr instancedir --generate tmp/${COLLECTION}_configs
cp conf/schema.xml tmp/${COLLECTION}_configs/conf/
solrctl --zk $ZK:2181/solr instancedir --create $COLLECTION tmp/${COLLECTION}_configs
solrctl --zk $ZK:2181/solr collection --create $COLLECTION -s $SHARD -r $REPLICA
solrctl --zk $ZK:2181/solr collection --list
