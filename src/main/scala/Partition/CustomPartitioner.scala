package Partition

import org.apache.flink.api.common.functions.Partitioner
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//TODO:自定义Flink分区
object CustomPartitioner{
  def main(args: Array[String]): Unit = {
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //todo:这里设置的并行度不能小于设置的分区数
    streamEnv.setParallelism(2)

    import org.apache.flink.streaming.api.scala._
    val stream = streamEnv.socketTextStream("192.168.213.60",2222)
    //转换计算
    val result = stream
      .map(x=>(x.toInt,x))
      .partitionCustom(new MyPartition,0) //0：需要分区的字段，这里代表第一个字段
      .map(x=>{
        println("当前线程id:" + Thread.currentThread().getId + ",value:" + x._1)
        x._1
      })


    result.print()

    streamEnv.execute()
  }

  //todo:如果key为偶数则发送到分区0，如果为奇数则发送到分区1
  class MyPartition extends Partitioner[Int]{
    override def partition(key: Int, number: Int): Int = {
//      println("分区数："+number)
      if(key%2==0){
        0
      }
      else {
        1
      }
    }
  }
}
