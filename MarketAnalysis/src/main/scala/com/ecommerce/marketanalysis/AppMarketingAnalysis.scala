package com.ecommerce.marketanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * APP 用户量统计
  */
object AppMarketingAnalysis {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val dataStream: DataStream[MarketingViewCount] = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp) //使用数据中时间升序作为时间时间
      .filter(_.behavior != "UNINSTALL") // 过滤到卸载用户
      .map(data => {
      ("dummyKey", 1L)
    }) // 包装成元祖， 后面做聚合统计
      .keyBy(_._1) // 将二元组（渠道和行为类型）相同的数据分组/分类到一起
      .timeWindow(Time.hours(1), Time.seconds(10)) // 统计一小时之内的， 每10秒做一次统计
      .aggregate(new CountAgg(), new MarketingCountTotal())// 增量聚合，来一个统计一个

    dataStream.print("=======>  ")
    env.execute("app marketing analysis job")
  }
}

class CountAgg extends AggregateFunction[(String, Long), Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 输入是预聚合函数的输出
class MarketingCountTotal extends WindowFunction[Long, MarketingViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    val startTs: String = new Timestamp(window.getStart).toString
    val endTs: String = new Timestamp(window.getEnd).toString
    val count: Long = input.iterator.next()
    out.collect(MarketingViewCount(startTs, endTs, "app marketing", "total", count))
  }
}