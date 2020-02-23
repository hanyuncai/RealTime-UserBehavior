package com.ecommerce

import java.net.URL
import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, _}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 使用flink cep 事件检测登录失败
  */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 1. 读取事件数据，创建简单事件流
    val resource: URL = getClass.getResource("/LoginLog.csv")
    val loginEventStream: KeyedStream[LoginEvent, Long] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      // 数据中时间有乱序， 使用自定义watermark 使数据按时间顺序处理, 处理时间延迟为5秒
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
      override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
    })
      .keyBy(_.userId)// 判断频繁登录使用用户ID做分组

    // 2. 定义对应的模式, 把模式应用到对应的事件流上
    val loginFailPattern: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail") // 第一个简单事件，第一次登录失败
      .next("next").where(_.eventType == "fail") // 严格行命令定义：紧邻（连续）的第二次登录失败
      .within(Time.seconds(2))// 事件在两秒之内

    // 3. 在事件流上应用模式， 得到一个pattern stream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream, loginFailPattern)

    // 4. 从pattern stream 上应用 select function, 检出匹配事件序列
    val loginFailDataStream: DataStream[Warning] = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print("===> ")

    env.execute("Login Fail With CEP Job")
  }
}

class LoginFailMatch extends PatternSelectFunction[LoginEvent, Warning]{
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // 从map中按照名称取出对应的事件
    val firstFail: LoginEvent = map.get("begin").iterator().next()
    val lastFail: LoginEvent = map.get("next").iterator().next()
    Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail")
  }
}