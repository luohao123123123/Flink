package Util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.io.PrintWriter
import java.net.URI
import scala.collection.mutable.HashSet

object HdfsUtil {
  val conf = new Configuration()
  conf.set("fs.defaultFS", "hdfs://master")
  conf.set("dfs.nameservices", "master")
  conf.set("dfs.ha.namenodes.hdfscluster", "nn1,nn2")
  conf.set("dfs.namenode.rpc-address.hdfscluster.nn1", "master:8020")
  conf.set("dfs.namenode.rpc-address.hdfscluster.nn2", "slave1:8020")
  conf.set("dfs.client.failover.proxy.provider.hdfscluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
  var hdfsRoot = "hdfs://master"
  var hdfs = FileSystem.get(URI.create(hdfsRoot), conf)

  def isPathExist(path:String)={
    hdfs.exists(new Path(path))
  }
  def getAbsolutePath(path:String) = {
    var absPath = hdfsRoot
    if(path.startsWith("/"))
      absPath += path
    else {
      absPath += "/"+path
    }
    absPath
  }

  def writeLines(lines:List[String], path: String) = {
    if(!isExist(path)){
      val writer = new PrintWriter(hdfs.create(new Path(path)))
      lines.foreach(writer.println(_))
      writer.close()
    }else{
      val writer = new PrintWriter(hdfs.append(new Path(path)))
      lines.foreach(writer.println(_))
      writer.close()
    }

  }

  def getSubdirs(path:String) = {
    //       hdfs.listStatus(new Path(path))
    //       .map(_.getPath.getName).toSet
    val it = hdfs.listFiles(new Path(path), true)
    val set = new HashSet[String]
    while(it.hasNext()) {
      val path = it.next().getPath
      set.add(path.getParent.getName+"/"+path.getName)
    }
    set
  }

  def delete(path:String) = {
    hdfs.delete(new Path(getAbsolutePath(path)), true)
  }

  def rename(src:String, dst:String) = {
    hdfs.rename(new Path(getAbsolutePath(src)), new Path(getAbsolutePath(dst)))
  }

  def isExist(path:String) ={
    hdfs.exists(new Path(getAbsolutePath(path)))
  }

}
