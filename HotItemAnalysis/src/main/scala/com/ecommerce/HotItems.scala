package com.ecommerce

import java.sql.Timestamp
import java.util.Properties

import com.ecommerce.hotItemBean.{ItemViewCount, UserBehavior}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val dataStream: DataStream[String] = env.readTextFile("D:\\workspace\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")
    //从kafka拿数据
    val kafkaProp: Properties = new Properties()
    kafkaProp.setProperty("bootstrap.servers", "master1.yunda:9092,slave1.yunda:9092,slave2.yunda:9092")
    kafkaProp.setProperty("acks", "all")
    kafkaProp.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    kafkaProp.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    kafkaProp.setProperty("group.id", "hot-item-group-id")

    val consumer: FlinkKafkaConsumer[String] = new FlinkKafkaConsumer[String]("hot_item", new SimpleStringSchema(), kafkaProp)
//    consumer.setStartFromEarliest()
    consumer.setStartFromLatest()

    val dataStream: DataStream[String] = env.addSource(consumer)

    val userDS: DataStream[UserBehavior] = dataStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
    })
      //数据中时间是递增的
      .assignAscendingTimestamps(_.timestamp * 1000L)//递增的 workmark

    val processDS: DataStream[String] = userDS.filter(_.behavior == "pv")
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))//窗口大小为1小时， 每5分钟统计一次
      .aggregate(new CountAgg(), new WindowResult()) //窗口聚合
      .keyBy(_.windowEnd) //按照窗口分组
      .process(new TopNHotItems(3))
    processDS

      .print("xxxxxxxxx=> ")

    env.execute("HotItemsJob")
  }
}

/**
  * 自定义预聚合函数
  */
class CountAgg() extends AggregateFunction[UserBehavior, Long, Long]{
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}

/**
  * 自定义预聚合函数计算平均数
  */
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double]{
  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (acc._1 + in.timestamp, acc._2 + 1)

  override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2

  override def merge(acc: (Long, Int), acc1: (Long, Int)): (Long, Int) = (acc._1 + acc1._1, acc._2 + acc1._2)
}

/**
  * 自定义窗口函数 输出ItemviewCount
  */
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

/**
  * 自定义处理函数
  * @param topSize
  */
class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String]{

  private var itemState: ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    itemState.add(value)//把每条数据存入状态列表
    //注册一个定时器
    context.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  //定时器触发时，对所有数据排序， 并输出结果
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //将所有state中的数据取出，放到一个list buffer 中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer[ItemViewCount]

    import scala.collection.JavaConversions._
    for(item <- itemState.get()){
      allItems += item
    }

    //按照count大小排序 并取前N个
    val sortItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    //清空状态
    itemState.clear()
    //将排名结果格式化输出
    val result: StringBuilder = new StringBuilder()
    result.append("时间： ").append(new Timestamp(timestamp - 1)).append("\n")

    //输出每一个商品的信息
    for (i <- sortItems.indices){
      val currentItem: ItemViewCount = sortItems(i)
      result.append("NO.").append(i + 1).append(":").append(" 商品ID=").append(currentItem.itemId)
        .append("浏览量=").append(currentItem.count).append("\n")
    }
    result.append("================================================")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}