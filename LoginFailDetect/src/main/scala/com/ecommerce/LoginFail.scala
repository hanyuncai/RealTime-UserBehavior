package com.ecommerce

import java.{lang, util}
import java.net.URL

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * 登录失败检测
  * 2秒钟之内用户频繁登录失败
  */
object LoginFail {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val resource: URL = getClass.getResource("/LoginLog.csv")
    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      // 数据中时间有乱序， 使用自定义watermark 使数据按时间顺序处理, 处理时间延迟为5秒
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
      override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
    })

    loginEventStream
        .keyBy(_.userId)// 判断频繁登录使用用户ID做分组
        .process(new LoginWarning(2))
        .print("==> ")

    env.execute("Login Fail Job")
  }
}
class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning]{
  // 定义状态， 保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    val loginFailList: lang.Iterable[LoginEvent] = loginFailState.get()
    // 判断类型是否是fail , 只添加fail 的事件到状态
    if (value.eventType == "fail"){
      if (! loginFailList.iterator().hasNext){//如果list为空，则是第一次登录失败
        // 注册定时器, 定时器触发时间单位为毫秒， 2秒后触发定时器
        // 这里存在的问题就是必须要两秒后才会触发操作， 实际要求只要满足2秒内错误两次就要触发，后面在 LoginFail2 中进行优化
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 2000L)
      }
      loginFailState.add(value)
    }else {
      // 如果没有失败，清空状态
      loginFailState.clear()
    }
  }
  // 触发定时器操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    // 根据状态里的个数决定是否输出报警
    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    val iter: util.Iterator[LoginEvent] = loginFailState.get().iterator()
    while (iter.hasNext){
      allLoginFails += iter.next()
    }

    // 判断个数
    if (allLoginFails.length >= maxFailTimes){
      out.collect(Warning(allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail in 2 seconds for " + allLoginFails.length + " times"))
    }
    // 清空状态
    loginFailState.clear()
  }
}

// 输入的登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)
// 输出的异常报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)