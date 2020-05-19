package com.cloudera.utils

import org.apache.kudu.client.KuduClient
import org.apache.kudu.spark.kudu.KuduContext

/**
  * package: com.cloudera.utils
  * describe: KuduClient 工具类
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/6/2
  * creat_time: 下午2:16
  * 公众号：Hadoop实操
  */
object KuduUtils extends Serializable{

  def getKuduClient(kuduContext: KuduContext): KuduClient = {
    val kuduClient = kuduContext.syncClient
    kuduClient
  }
}
