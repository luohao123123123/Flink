package flink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//TODO:Flink读取集合数据
object CollectionToFlinkSource {
  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    streamEnv.setParallelism(1)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._


    //读取数据
    val dataStream: DataStream[StationLog] = streamEnv.fromCollection(Array(
       StationLog("001", "186", "189", "busy", 1577071519462L, 0),
       StationLog("002", "186", "188", "busy", 1577071520462L, 0),
       StationLog("003", "183", "188", "busy", 1577071521462L, 0),
       StationLog("004", "186", "188", "success", 1577071522462L, 32)
    ))
    dataStream.print()


    streamEnv.execute()
  }
}
