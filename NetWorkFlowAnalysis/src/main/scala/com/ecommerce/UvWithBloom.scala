package com.ecommerce

import java.lang
import java.net.URL

import com.ecommerce.UniqueVisitor.getClass
import com.ecommerce.netWorkFlowBean.{UserBehavior, UvCount}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

/**
  * 使用布隆过滤器处理UV数据
  * 基于redis处理数据
  */
object UvWithBloom {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //用相对路径定义数据源
    val resource: URL = getClass.getResource("/UserBehavior.csv")
    env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000) //升序数据 * 1000 表示秒
      .filter(_.behavior == "pv") //只统计pv操作
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())//如果不定义trigger窗口触发函数，则窗口要等到收集满了之后才触发计算
      .process(new UvCountWithBloom())


    env.execute("Uv with bloom job")
  }
}

/**
  * 自定义窗口触发器
  */
class MyTrigger() extends Trigger[(String, Long), TimeWindow]{
  /**
    * 当元素到来时，触发此方法
    * FIRE_AND_PURGE : 每来一条数据触发一次，并且清空所有窗口中的数据状态避免撑爆内存
    * @param element
    * @param timestamp
    * @param window
    * @param ctx
    * @return
    */
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  /**
    * 事件事件是 event time 触发这个方法
    * 与watermark 有关
    * @param time
    * @param window
    * @param ctx
    * @return
    */
  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = ???
}
//定义一个布隆过滤器(位图存储)，将hash值存到不通位图（避免碰撞）
//可以定义一个默认大小
class Bloom(size: Long) extends Serializable{
  //位图的总大小 , 默认 1 << 27 （左移2的27次方， 16M, 1后面27个0）
  private val cap = if (size > 0) size else 1 << 27

  //定义简单hash函数，复杂的hash函数可以调用库或者多使用几次hash
  def hash(value: String, seed: Int): Long ={
    var result: Long = 0L
    for (i <- 0 until value.length){
      result = result + seed + value.charAt(i)
    }
    //只取后面27位，cap-1 为前面一位为0，后面27个1，&运算的逻辑是两个全是1则输出1，否则输出0
    result & (cap - 1)
  }
}

class UvCountWithBloom extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{
  //定义jedis
  lazy val jedis = new Jedis("192.168.1.7", 6379)
  //默认64M, 可以处理5亿多个Key
  lazy val bloom = new Bloom(1<<29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //位图的存储方式,   key是windowEnd, value是bitmap
    val storeKey: String = context.window.getEnd.toString
    var count = 0L //计数
    //把每个窗口的uv count值也存入名为count的redis表, 存放内容为（windowEnd -> uvCount) 所以要先从redis 中读取
    if(jedis.hget("count", storeKey) != null){
      count = jedis.hget("count", storeKey).toLong
    }

    //用布隆过滤器判断当前用户是否已经存在
    val userId: String = elements.last._2.toString
    //seed 随意给一个随机数种子，给一个允许范围内的尽量大一点的质数， 以至于尽量分散一些
    val offset = bloom.hash(userId, 61)
    //定义一个标志位，判断redis位图中有没有这一位
    val isExist: lang.Boolean = jedis.getbit(storeKey, offset)
    if (isExist){
      //如果不存在， 位图对应位置1。 count + 1
      jedis.setbit(storeKey, offset, true)
      jedis.hset("count", storeKey, (count + 1).toString)

      out.collect(UvCount(storeKey.toLong, count + 1))
    } else {
      out.collect(UvCount(storeKey.toLong, count))
    }
  }
}