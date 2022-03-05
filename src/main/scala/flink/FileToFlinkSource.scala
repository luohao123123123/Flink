package flink

import Util.HdfsUtil
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

//TODO:Flink读取Hdfs数据
object FileToFlinkSource {
  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    streamEnv.setParallelism(1)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._


    //读取数据
    val stream = streamEnv.readTextFile(HdfsUtil.getAbsolutePath("wordcount.txt"))
    //转换计算
    val result: DataStream[(String, Int)] = stream
      .flatMap(_.split(","))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    //打印结果到控制台
    result.print()
    //启动流式处理，如果没有该行代码上面的程序不会运行
    streamEnv.execute("wordcount")
  }
}
