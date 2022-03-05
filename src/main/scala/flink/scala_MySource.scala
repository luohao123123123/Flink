package flink

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class scala_MySource extends SourceFunction[StationLog] {
  //是否终止数据流的标记
  var flag = true;

  override def run(sourceContext: SourceFunction.SourceContext[StationLog]):
  Unit = {
    val random = new Random()
    var types = Array("fail", "busy", "barring", "success")
    while (flag) { //如果流没有终止，继续获取数据
      1.to(5).map(i => {
        var callOut = "1860000%04d".format(random.nextInt(10000))
        var callIn = "1890000%04d".format(random.nextInt(10000))
        new StationLog("station_" + random.nextInt(10), callOut, callIn, types(random.nextInt(4)), System.currentTimeMillis(), 0)
      }).foreach(sourceContext.collect) //发数据
      Thread.sleep(3000) //每发送一次数据休眠2秒
    }
  }

  //终止数据流
  override def cancel(): Unit = flag = false

}

case class StationLog(sid: String, callOut: String, callIn: String, callType: String, callTime: Long, duration: Long)
