package flink

import Util.MysqlUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.sql.{Connection, DriverManager, PreparedStatement}

//TODO；自定义Flink数据源写入Mysql
object FlinkToCustomSink {
  //自定义sink，写入其他容器
  class  MyCustomSink extends RichSinkFunction[StationLog]{
    var conn: Connection = _
    var pst: PreparedStatement = _
    //生命周期管理，在Sink初始化的时候调用
    override def open(parameters: Configuration): Unit = {
      //todo：这里写需要写入数据的连接
      conn =  MysqlUtil.getConnection
      pst = conn.prepareStatement("insert into table1(name,addr) values(?, ?)")
    }

    override def invoke(value: StationLog, context: SinkFunction.Context): Unit = {
      pst.setString(1,value.callOut)
      pst.setString(2,value.callType)
      pst.executeUpdate()

    }

    override def close(): Unit = {
      MysqlUtil.closeConnection(conn)
      MysqlUtil.closeStatement(pst)
    }

  }


  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment

    streamEnv.setParallelism(1)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._
    val data: DataStream[StationLog] = streamEnv.addSource(new
        scala_MySource)
    data.print()
    //数据写入msyql
    data.addSink(new MyCustomSink)

    streamEnv.execute()
  }

}
