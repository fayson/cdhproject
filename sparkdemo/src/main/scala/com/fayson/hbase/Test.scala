package com.fayson.hbase

import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
  * package: com.fayson.hbase
  * describe: TODO
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2019/1/6
  * creat_time: 下午11:31
  * 公众号：Hadoop实操
  */
object Test {

  def main(args: Array[String]): Unit = {
    val quorum = "cdh1.fayson.com,cdh2.fayson.com,cdh3.fayson.com"
    val clientPort = "2181"
    val hBaseTempTable = "ods_user_hbase"

    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum", quorum)
    hBaseConf.set("hbase.zookeeper.property.clientPort", clientPort)

    val connection = ConnectionFactory.createConnection(hBaseConf)
    val hBaseTableName = TableName.valueOf(hBaseTempTable)
    val admin = connection.getAdmin
    if (!admin.tableExists(hBaseTableName)) {
      val tableDesc = new HTableDescriptor(hBaseTempTable)
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes))
      admin.createTable(tableDesc)
    }

  }

}
