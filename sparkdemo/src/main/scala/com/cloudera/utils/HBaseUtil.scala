package utils

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
  * package: utils
  * describe: HBase工具类
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/5/28
  * creat_time: 上午10:51
  * 公众号：Hadoop实操
  */
object HBaseUtil extends Serializable {

  /**
    * @param zkList Zookeeper列表已逗号隔开
    * @param port ZK端口号
    * @return
    */
  def getHBaseConn(zkList: String, port: String): Connection = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zkList)
    conf.set("hbase.zookeeper.property.clientPort", port)
    val connection = ConnectionFactory.createConnection(conf)
    connection
  }

}
