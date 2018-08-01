package com.cloudera.hbase

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.spark.{HBaseContext, KeyFamilyQualifier}
import org.apache.hadoop.hbase.spark.HBaseRDDFunctions._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job

import scala.collection.mutable

/**
  * package: com.cloudera.hbase
  * describe: 使用BulkLoad的方式将Hive数据导入HBase
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/7/31
  * creat_time: 下午2:04
  * 公众号：Hadoop实操
  */
object Hive2HBase {

  def main(args: Array[String]) {

    //库名、表名、rowKey对应的字段名、批次时间、需要删除表的时间参数
    val rowKeyField = "id"
    val quorum = "cdh01.fayson.com,cdh02.fayson.com,cdh03.fayson.com"
    val clientPort = "2181"
    val hBaseTempTable = "ods_user_hbase"

    val sparkConf = new SparkConf().setAppName("Hive2HBase")
    val sc = new SparkContext(sparkConf)

    val hiveContext = new HiveContext(sc)
    //从hive表读取数据
    val datahiveDF = hiveContext.sql(s"select * from ods_user")

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

    //将DataFrame转换bulkload需要的RDD格式
    val rddnew = datahiveDF.rdd.map(row => {
      val rowKey = row.getAs[String](rowKeyField)

      fields.map(field => {
        val fieldValue = row.getAs[String](field)
        (Bytes.toBytes(rowKey), Array((Bytes.toBytes("info"), Bytes.toBytes(field), Bytes.toBytes(fieldValue))))
      })
    }).flatMap(array => {
      (array)
    })

    //使用HBaseContext的bulkload生成HFile文件
    hbaseContext.bulkLoad[Put](rddnew.map(record => {
      val put = new Put(record._1)
      record._2.foreach((putValue) => put.addColumn(putValue._1, putValue._2, putValue._3))
      put
    }), TableName.valueOf(hBaseTempTable), (t : Put) => putForLoad(t), "/tmp/bulkload")

    val conn = ConnectionFactory.createConnection(hBaseConf)
    val hbTableName = TableName.valueOf(hBaseTempTable.getBytes())
    val regionLocator = new HRegionLocator(hbTableName, classOf[ClusterConnection].cast(conn))
    val realTable = conn.getTable(hbTableName)
    HFileOutputFormat2.configureIncrementalLoad(Job.getInstance(), realTable, regionLocator)

    // bulk load start
    val loader = new LoadIncrementalHFiles(hBaseConf)
    val admin = conn.getAdmin()
    loader.doBulkLoad(new Path("/tmp/bulkload"),admin,realTable,regionLocator)

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
