package com.cloudera.streaming

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, HTable, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{Logging}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

/**
  * package: com.cloudera.streaming
  * describe: 自定义Receiver类用于提供SparkStreaming的DataStream数据源
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/1/9
  * creat_time: 上午12:21
  * 公众号：Hadoop实操
  */
class MyReceiver(zkHost: String, zkPort: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) with Logging {

  override def onStart(): Unit =  {
    receive()
  }

  override def onStop(): Unit = {

  }

  private def receive(): Unit =  {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkHost)
    conf.set("hbase.zookeeper.property.clientPort", zkPort)
    val connection = ConnectionFactory.createConnection(conf);

    val admin = connection.getAdmin;

    val tableName = "picHbase"
    val table = new HTable(conf, tableName)
    val scan = new Scan()
    scan.setCaching(1)

    val rs = table.getScanner(scan)
    val iterator = rs.iterator()
    while(iterator.hasNext) {
      val result = iterator.next();
      val b = new StringBuilder
      b.append(Bytes.toString(result.getRow))
      b.append(",")

      val cells = result.listCells()
      val it = cells.iterator()
      while (it.hasNext) {
        val kv = it.next()
        b.append(Bytes.toString(kv.getValue))
        b.append(",")
        b.append(kv.getTimestamp)
      }
      store(b.toString())
    }

    restart("Trying to connect again")
    table.close()
    connection.close()
  }
}
