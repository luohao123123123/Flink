package flink

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object scala_FlinkWriteToSocket {
  def main(args: Array[String]): Unit = {
    val streamEnv=StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1)
    import org.apache.flink.streaming.api.scala._


    val data = streamEnv.addSource(new scala_MySource_02)
    data.print()
    data.writeToSocket("192.168.213.60",2222,new SimpleStringSchema())

    streamEnv.execute()
  }
}
