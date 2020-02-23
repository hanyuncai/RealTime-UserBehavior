package com.ecommerce

import java.net.URL

import org.apache.flink.streaming.api.scala._
import com.ecommerce.netWorkFlowBean.{UserBehavior, UvCount}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * 统计UV (去重)
  */
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //用相对路径定义数据源
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val dateStream: DataStream[UvCount] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000) //升序数据 * 1000 表示秒
      .filter(_.behavior == "pv") //只统计pv操作
      .timeWindowAll(Time.hours(1)) //1小时的滚动时间窗口
      .apply(new UvCountByWindow())
    dateStream.print("=====> ")


    env.execute("Page View Job")
  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 定义一个scala set 用于保存 所有的数据userId并去重
    //set默认是不可变的， 不重复的集合, 但是存在的问题是 数据量大，内存会爆掉
    //优化方法：避免内存容量的问题，将是否存在集合中，用一个二进制位保存到位图中，从而压缩存储（布隆过滤器）
    var idSet: Set[Long] = Set[Long]()
    //把当前窗口所有的数据id 手机到set中， 最后输出set的大小
    for(userBehavior <- input){
      idSet += userBehavior.userId
    }
    out.collect(UvCount(window.getEnd, idSet.size))
  }
}
