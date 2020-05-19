package com.fayson.hbase

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.spark.{HBaseContext, KeyFamilyQualifier}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * package: com.fayson.hbase
  * describe: 使用Bulkload的方式优化HBase写入性能问题
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2019/1/6
  * creat_time: 下午10:47
  * 公众号：Hadoop实操
  */
object Hive2HBaseOptimalize {

  def main(args: Array[String]) {

    //库名、表名、rowKey对应的字段名、批次时间、需要删除表的时间参数
    val rowKeyField = "id"
    val quorum = "cdh1.fayson.com,cdh2.fayson.com,cdh3.fayson.com"
    val clientPort = "2181"
    val hBaseTempTable = "ods_user_hbase"

    val sparkConf = new SparkConf().setAppName("Hive2HBaseOptimalize")
    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)
    hiveContext.refreshTable("ods_user")
    //从hive表读取数据
    val datahiveDF = hiveContext.sql(s"select * from ods_user")


    println("ods_user count:=======" + datahiveDF.count())

    //表结构字段
    var fields = datahiveDF.columns

    //去掉rowKey字段
    fields = fields.dropWhile(_ == rowKeyField)

    val hBaseConf = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum", quorum)
    hBaseConf.set("hbase.zookeeper.property.clientPort", clientPort)

    //表不存在则建Hbase临时表
    creteHTable(hBaseTempTable, hBaseConf)

    val hbaseContext = new HBaseContext(sc, hBaseConf)


    /**
      * 合并多个Column为一个Column
      * @param iter
      * @return
      */
    def mergeColumn(iter : Iterator[Row]) : Iterator[Put] = {
      val res = List[Put]()

      while (iter.hasNext) {
        val row = iter.next()
        val rowKey = row.getAs[String](rowKeyField)

        val map : mutable.HashMap[String,Object]= mutable.HashMap()
        fields.foreach(field => {
          val fieldValue = row.getAs[String](field)
          map.+=((field , fieldValue))
        })

        println("Map Size: ===============" + map.size)


        val put = new Put(Bytes.toBytes(rowKey))

        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("user"), Bytes.toBytes(map.toMap.toString()))

        res.::(put)
      }
      res.iterator
    }

    //将DataFrame转换bulkload需要的RDD格式
    val rdd = datahiveDF.mapPartitions(mergeColumn)

    hbaseContext.bulkLoad[Put](rdd,
      TableName.valueOf(hBaseTempTable),
      (put: Put) => putForLoad(put),
      "/tmp/bulkload")

    sc.stop()
  }

  /**
    * 创建HBase表
    * @param tableName 表名
    */
  def creteHTable(tableName: String, hBaseConf : Configuration) = {
    val connection = ConnectionFactory.createConnection(hBaseConf)
    val hBaseTableName = TableName.valueOf(tableName)
    val admin = connection.getAdmin
    if (!admin.tableExists(hBaseTableName)) {
      val tableDesc = new HTableDescriptor(hBaseTableName)
      tableDesc.addFamily(new HColumnDescriptor("info".getBytes))
      admin.createTable(tableDesc)
    }
    connection.close()
  }

  /**
    * Prepare the Put object for bulkload function.
    * @param put The put object.
    * @throws java.io.IOException
    * @throws java.lang.InterruptedException
    * @return Tuple of (KeyFamilyQualifier, bytes of cell value)*/
  @throws(classOf[IOException])
  @throws(classOf[InterruptedException])
  def putForLoad(put: Put): Iterator[(KeyFamilyQualifier, Array[Byte])] = {
    val ret: mutable.MutableList[(KeyFamilyQualifier, Array[Byte])] = mutable.MutableList()
    import scala.collection.JavaConversions._
    for (cells <- put.getFamilyCellMap.entrySet().iterator()) {
      val family = cells.getKey
      for (value <- cells.getValue) {
        val kfq = new KeyFamilyQualifier(CellUtil.cloneRow(value), family, CellUtil.cloneQualifier(value))
        ret.+=((kfq, CellUtil.cloneValue(value)))
      }
    }
    ret.iterator
  }

}
