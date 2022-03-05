package flink

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.util.Random

//TODO:Flink读取自定义数据源
object CustomToFlinkSource {
  def main(args: Array[String]): Unit = {
    val streamEnv=StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._

    val data = streamEnv.addSource(new scala_MySource)
    data.print()


    streamEnv.execute()
  }


}
