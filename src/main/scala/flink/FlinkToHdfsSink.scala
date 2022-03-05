package flink

import Util.HdfsUtil
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.datastream.DataStreamSource
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy

//TODO:Flink自定义数据源到Hdfs
object FlinkToHdfsSink {
  def main(args: Array[String]): Unit = {
    //创建连接
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    streamEnv.setParallelism(1)
    //导入隐式转换
    import org.apache.flink.streaming.api.scala._

    // 数据源
    val data: DataStreamSource[StationLog] = streamEnv.addSource(new scala_MySource)

    //创建一个文件滚动规则
    val rolling: DefaultRollingPolicy[StationLog, String] = DefaultRollingPolicy.create()
      .withInactivityInterval(2000) //不活动的间隔时间。
      .withRolloverInterval(2000) //每隔两秒生成一个文件 ，重要
      .build()

    //创建一个HDFS Sink
    var hdfsSink = StreamingFileSink.forRowFormat[StationLog](
      // 注意此处是flink的Path
      new Path(HdfsUtil.getAbsolutePath("/FlinkToHdfs")), new SimpleStringEncoder[StationLog]("UTF-8"))
      .withBucketCheckInterval(1000) //检查分桶的间隔时间
      //            .withBucketAssigner(new MemberBucketAssigner)
      .withRollingPolicy(rolling)
      .build()

    // 添加sink
    data.print()
    data.addSink(hdfsSink)


    streamEnv.execute()
  }
}
