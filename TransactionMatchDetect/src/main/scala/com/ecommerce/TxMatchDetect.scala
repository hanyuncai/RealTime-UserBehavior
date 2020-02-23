package com.ecommerce

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
  * 对账
  */
object TxMatchDetect {

  //定义侧输出流tag
  val unmatchedPays: OutputTag[OrderEvent] = new OutputTag[OrderEvent]("unmatchedPays")
  val unmatchedReceipts: OutputTag[ReceiptEvent] = new OutputTag[ReceiptEvent]("unmatchedReceipts")

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取订单事件流
    val resource: URL = getClass.getResource("/OrderLog.csv")
    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .filter(_.txId != "")
      .assignAscendingTimestamps(_.eventTime * 1000L)
      .keyBy(_.txId)

    // 读取支付到账事件流
    val receiptResource: URL = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStrem: KeyedStream[ReceiptEvent, String] = env.readTextFile(receiptResource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        ReceiptEvent(dataArray(0).trim, dataArray(1).trim, dataArray(2).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime * 1000L)// 两条流的watermark，以慢的那条流的时间为watermark的时间
      .keyBy(_.txId)

    // 将两条流连接起来， 共同处理
    val processedStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream.connect(receiptEventStrem)
      .process(new TxPayMatch())

    processedStream.print("matched=>")
    processedStream.getSideOutput(unmatchedPays).print("unmatchedPays=>")
    processedStream.getSideOutput(unmatchedReceipts).print("unmatchedReceipts=>")


    env.execute("transaction match detect job")
  }

  class TxPayMatch extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]{
    // 定义状态来保存已经到达的订单支付事件和到账事件
    lazy val payState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state", classOf[OrderEvent]))
    lazy val receiptState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state", classOf[ReceiptEvent]))

    // 订单支付事件数据的处理
    override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 判断有没有对应的到账事件
      val receipt: ReceiptEvent = receiptState.value()
      if (receipt != null){
        // 如果已经有receipt, 在主流输出匹配信息, 清空状态
        out.collect((pay, receipt))
        receiptState.clear()
      }else{
        // 如果还没到，那么把pay存入状态，并且注册一个定时器等待,等待5秒，5秒后告警
        payState.update(pay)
        ctx.timerService().registerEventTimeTimer(pay.eventTime * 1000L + 5000L)
      }
    }

    // 到账事件的数据处理
    override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 同样的处理流程
      val pay: OrderEvent = payState.value()
      if (pay != null){
        out.collect((pay, receipt))
        payState.clear()
      }else{
        receiptState.update(receipt)
        ctx.timerService().registerEventTimeTimer(receipt.eventTime * 1000L + 5000L)
      }
    }

    override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
      // 到时见了，还没有收到某个事件， 那么输出报警信息
      if (payState.value() != null){
        // receipt 没来, 输出pay到侧输出流
        ctx.output(unmatchedPays, payState.value())
      }
      if (receiptState.value() != null){
        // pay 没来, 输出receipt到侧输出流
        ctx.output(unmatchedReceipts, receiptState.value())
      }
      payState.clear()
      receiptState.clear()
    }
  }

}
// 接收流事件样例类
case class ReceiptEvent(txId: String, payChannel: String, eventTime: Long)
// 输入订单事件的样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)