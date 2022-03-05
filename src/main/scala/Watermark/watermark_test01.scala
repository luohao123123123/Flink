package Watermark

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.table.shaded.com.ibm.icu.text.SimpleDateFormat
import org.apache.flink.util.Collector

//TODO:处理乱序数据，watermark+window，在window通过Envent time 对数据进行排序
object watermark_test01 {
  def main(args: Array[String]): Unit = {
    val streamEnv=StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val input=streamEnv.socketTextStream("192.168.213.60",2222)
    import org.apache.flink.streaming.api.scala._
    val dataMap = input.map(x => {
      val data=x.split(",")(0)
      val time=x.split(",")(1).toLong
      (data, time)
    })

    val watermark=dataMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      var currentMaxTimestamp = 0L
      val maxOutOfOrderness = 10000L//最大允许的乱序时间是10s

      var a : Watermark = null

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

      override def getCurrentWatermark: Watermark = {
        a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        a
      }

      override def extractTimestamp(t: (String, Long), l: Long): Long = {
        val timestamp = t._2
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        println("timestamp:" + t._1 +","+ t._2 + "|" +format.format(t._2) +","+  currentMaxTimestamp + "|"+ format.format(currentMaxTimestamp) + ","+ a.toString)
        timestamp
      }
    })
//    00007,2461724781000

    val window = watermark
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new WindowFunctionTest)
    window.print()

    streamEnv.execute()
  }
  class WindowFunctionTest extends WindowFunction[(String,Long),(String, Int,String,String,String),String,TimeWindow]{

    override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int,String,String,String)]): Unit = {
      val list = input.toList.sortBy(_._2)  //todo:把乱序的事件时间排序，使得完全按Event time 去处理，以达到处理乱序数据的目的
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      out.collect(key,input.size,format.format(list.head._2),format.format(window.getStart),format.format(window.getEnd))
    }
  }
}
