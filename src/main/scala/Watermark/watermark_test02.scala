package Watermark

import flink.scala_MySource_03
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
object watermark_test02 {
  def main(args: Array[String]): Unit = {
    val streamEnv=StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.streaming.api.scala._
    val input: DataStream[(String, Long)] = streamEnv.addSource(new scala_MySource_03)


    val dataMap = input.map(x => {
      val data=x._1
      val time=x._2
      (data, time)
    })

    val watermark=dataMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String, Long)] {
      var currentMaxTimestamp = 0L
      val maxOutOfOrderness = 5000L//最大允许的乱序时间是5s

      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")


      override def getCurrentWatermark: Watermark = {
        new Watermark(currentMaxTimestamp - maxOutOfOrderness)
      }

      override def extractTimestamp(t: (String, Long), l: Long): Long = {
        val timestamp = t._2
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        println("value:"+t._1,"timestamp:"+format.format(timestamp),"currentMaxTimestamp:"+format.format(currentMaxTimestamp))
        timestamp
      }
    })
//   3,2461723172000

    val window = watermark
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .apply(new WindowFunctionTest)
    window.print()

    streamEnv.execute()
  }

  class WindowFunctionTest extends WindowFunction[(String,Long),(String,String),String,TimeWindow]{

    override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String,String)]): Unit = {
      val list = input.toList.sortBy(_._2)  //todo:把乱序的事件时间排序，使得完全按Event time 去处理，以达到处理乱序数据的目的
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      out.collect(key,format.format(list.head._2))
    }
  }
}
