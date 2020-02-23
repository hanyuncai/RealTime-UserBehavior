package com.ecommerce

import java.sql.Timestamp
import java.util
import java.util.{Date, Locale}

import com.ecommerce.netWorkFlowBean.{ApacheLogEvent, UrlViewCount}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * analysis hot urls top 5
  */
object NetworkFlow {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[String] = env.readTextFile("D:\\workspace\\UserBehaviorAnalysis\\NetWorkFlowAnalysis\\src\\main\\resources\\apache.log")
//    dataStream.print("=======> ")

    dataStream.map(data => {
      val array: Array[String] = data.split(" ")
      val timeStr: String = array(3) + array(4)
      val format: FastDateFormat = FastDateFormat.getInstance("dd/MMM/yyyy:hh:mm:ssZ", Locale.ENGLISH)
      val dateStr: String = timeStr.substring(timeStr.indexOf("[")+1, timeStr.lastIndexOf("]"))
      val date: Date = format.parse(dateStr)

      ApacheLogEvent(array(0).trim, array(6).trim, date.getTime, array(9).trim, array(11).trim)
    })
      //时间乱序， 使用BoundedOutOfOrdernessTimestampExtractor 指定
      //周期性生成watermark, 默认周期是200毫秒
        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
          override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
        })
        .keyBy(_.url)
      //十分钟一个窗口， 每个窗口5秒钟计算一次
        .timeWindow(Time.minutes(10), Time.seconds(5))
      //允许60秒的迟到数据去更新窗口
        .allowedLateness(Time.seconds(60))
    //自定义聚合函数
        .aggregate(new CountAgg(), new WindowResult())
    //聚合完之后，基于windowEnd再分组
        .keyBy(_.windowEnd)
    //排序输出
        .process(new TopNHotUrls(5)).print("===============>  ")

    env.execute("NetworkFlow")
  }
}

/**
  * 自定义排序输出处理函数
  * @param topSize
  */
class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{
  //获取当前状态数据
  lazy private val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-state", classOf[UrlViewCount]))

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    urlState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //从状态中拿到数据
    val allUrlViews: ListBuffer[UrlViewCount] = new ListBuffer[UrlViewCount]()
    val iter: util.Iterator[UrlViewCount] = urlState.get().iterator()
    while (iter.hasNext){
      allUrlViews += iter.next()
    }

    urlState.clear()
    //排序
    val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortWith(_.count > _.count).take(topSize)
    //格式化输出
    val result: StringBuilder = new StringBuilder()
    result.append("时间: ").append(new Timestamp( timestamp - 1)).append("\n")

    for (i <- sortedUrlViews.indices){
      val currentUrlView: UrlViewCount = sortedUrlViews(i)
      result.append("NO. ").append(i + 1).append(":")
        .append(" URL= ").append(currentUrlView.url)
        .append(" 访问量=").append(currentUrlView.count).append("\n")
    }
    result.append("======================================")
    Thread.sleep(1000)

    out.collect(result.toString())

  }
}

/**
  * 自定义预聚合函数
  */
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

/**
  * 自定义窗口函数
  */
class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}