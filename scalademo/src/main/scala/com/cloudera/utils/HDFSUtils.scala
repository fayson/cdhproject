package com.cloudera.utils

import org.apache.hadoop.fs.permission._
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.JavaConversions._


/**
  * package: com.cloudera.utils
  * describe: 用户操作HDFS工具类
  * creat_user: Fayson 
  * email: htechinfo@163.com
  * creat_date: 2018/11/13
  * creat_time: 下午10:05
  * 公众号：Hadoop实操
  */
object HDFSUtils {

  /**
    * 使用HDFS API向HDFS创建目录
    * 在创建目录指定目录权限为777时，该权限需要与HDFS默认的umask权限相减，最终得出目录权限为755
    * umask默认为022，0表示对owner没有限制，2表示对group不允许有写权限，2表示对other不允许有写权限
    * 因此在创建目录指定777，但创建出来的目录为755的原因
    * @param fileSystem
    * @param dirName
    */
  def mkdir(fileSystem: FileSystem, dirName: String):Unit = {
    val path = new Path(dirName)
    if(fileSystem.exists(path)) {
      System.out.println("目录已存在")
    } else {
      val isok = fileSystem.mkdirs(path)
      if(isok) {
        System.out.println("HDFS目录创建成功:" + dirName)
      } else {
        System.out.println("HDFS目录创建失败:" + dirName)
      }
    }
  }

  /**
    * 设置HDFS指定目录及文件权限
    * @param fileSystem
    * @param path  文件或目录路径
    * @param mode  权限模式，如:777、755、644，数字对应的R=4,W=2,X=1
    */
  def setPermission(fileSystem: FileSystem, path: String, mode: String): Unit = {
    val fspath = new Path(path)
    fileSystem.setPermission(fspath, new FsPermission(mode))
  }

  /**
    * 设置HDFS指定目录或文件的属主及属组
    * @param fileSystem
    * @param path
    * @param username
    * @param groupname
    */
  def setowner(fileSystem: FileSystem, path: String, username: String, groupname: String): Unit = {
    val fspath = new Path(path)
    fileSystem.setOwner(fspath, username, groupname)
  }

  /**
    * 设置HDFS指定目录的ACL权限
    * 在指定ACL时AclEntryScope.ACCESS表示当前目录所拥有的访问权限
    * AclEntryScope.DEFAULT，表示该目录下所有子目录及文件集成父目录的Default ACL权限
    * @param fileSystem
    * @param path
    */
  def setAcl(fileSystem: FileSystem,path: String): Unit = {
    val fspath = new Path(path)
    val listAcl = List[AclEntry](
      new AclEntry.Builder().setType(AclEntryType.GROUP).setScope(AclEntryScope.ACCESS).setName("testa").setPermission(FsAction.ALL).build()
    )

    fileSystem.modifyAclEntries(fspath, listAcl)
  }

  /**
    * 递归指定路径下所有目录及文件
    * @param path
    * @param fileSystem
    * @return
    */
  def recursiveDir(path: String, fileSystem: FileSystem): List[Path] = {
    var listPath = List[Path]()
    val fspath = new Path(path)
    val listfiles = fileSystem.listStatus(fspath)
    listfiles.foreach(f => {
      System.out.println(f.getPath.toString)
      if(f.isDirectory) {
        recursiveDir(f.getPath.toString, fileSystem)
      }
    })
    listPath
  }

}
