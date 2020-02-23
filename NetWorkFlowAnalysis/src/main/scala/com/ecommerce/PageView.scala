package com.ecommerce

import java.net.URL

import com.ecommerce.netWorkFlowBean.UserBehavior
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 统计PV
  */
object PageView {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //用相对路径定义数据源
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    val dataStream: DataStream[(String, Int)] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000) //升序数据 * 1000 表示秒
      .filter(_.behavior == "pv") //只统计pv操作
      .map(data => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1)) //窗口大小是一个小时, 1小时输出一次
      .sum(1)
    dataStream.print("pv count ==> ")


    env.execute("Page View Job")
  }
}
