/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.examples.scala

import java.lang.{Long => JLong}
import java.text.SimpleDateFormat
import java.util.{Date, TimeZone}

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.Window
import org.apache.flink.streaming.api.{TimeCharacteristic, datastream}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{BkDataGroupWindowAggregateStreamQueryConfig, BkDataSideOutputProcess, TableEnvironment, TableException}
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row

object AllowedLatenessSQLExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
    val utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

    val ds: DataStream[(Long, String, Int, Long)] = env
      .addSource(new SourceFunction[(Long, String, Int, Long)]() {

        val serialVersionUID = 1L;

        var initTime = System.currentTimeMillis();

        var c = 0;

        def run(ctx: SourceFunction.SourceContext[(Long, String, Int, Long)]): Unit = {
          for (i <- 0 to 62) {
            ctx.collect((2L, "rubber", 1, dateFormat.parse("2019-01-01 00:00:00").getTime + i * 30 * 1000L))
            Thread.sleep(100)
          }
          println("-----delay data------")
          // 延迟数据
          for (i <- 0 to 600) {
            ctx.collect((3L, "rubber", 1, dateFormat.parse("2019-01-01 00:00:00").getTime + i * 30 * 1000L))
            Thread.sleep(100)
            if (System.currentTimeMillis() - initTime >= 10 * 1000) {
              println("====================10 second====================")
              initTime = System.currentTimeMillis()
              // 特定延迟数据
              ctx.collect((4L, "rubber", 1, utcFormat.parse("2018-12-31 16:39:20").getTime() + 300 * 1000L * c))
              println((4L, "rubber", 1, utcFormat.format(new Date(utcFormat.parse("2018-12-31 16:39:20").getTime() + 300 * 1000L * c))))
              c += 1
            }
          }
          println("----------------------------------------")
          for (i <- 0 until 100) {
            Thread.sleep(1000);
          }
        }

        def cancel() {}
      }).assignTimestampsAndWatermarks(new MyWatermarkExtractor)

    // register the date stream under the name "orders"
    tableEnv.registerDataStream("Orders", ds, 'user, 'product, 'amount, 'logtime,
      'proctime.proctime, 'rowtime.rowtime)

    val table = tableEnv.sqlQuery("\n" +
      """
        |SELECT
        |  user,
        |  TUMBLE_START(rowtime, INTERVAL '5' minute) as wStart,
        |  SUM(amount)
        |FROM Orders
        |GROUP BY TUMBLE(rowtime, INTERVAL '5' minute), user
      """.stripMargin
    )

    val queryConfig: BkDataGroupWindowAggregateStreamQueryConfig = new BkDataGroupWindowAggregateStreamQueryConfig

    // TODO：自定义延迟1小时内的数据，按处理时间分钟聚合后输出一个增量值
    queryConfig.allowedLateness(Time.hours(1).toMilliseconds)
    queryConfig.trigger(AllowedLatenessEventTimeTrigger.of())

    // 设置侧输出延迟数据输出标签
    queryConfig.sideOutputLateData("OutputTag-test")
    // 设置侧输出延迟数据的处理器
    queryConfig.setSideOutputProcess(new BkDataSideOutputProcess {
      override def process(dataStream: datastream.DataStream[CRow], inputSchema: RowSchema): Unit = {
        val rowTimeIdx = inputSchema.fieldNames.indexOf("rowtime")
        if (rowTimeIdx < 0) {
          throw new TableException("Time attribute could not be found. This is a bug.")
        }
        dataStream.map(new MapFunction[CRow, JTuple2[String, Long]] {
          /**
           * The mapping method. Takes an element from the input data set and transforms
           * it into exactly one element.
           *
           * @param value The input value.
           * @return The transformed value
           * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
           *                   to fail and may trigger recovery.
           */
          override def map(value: CRow): JTuple2[String, Long] = {
            val rowTime = new Date(value.row.getField(rowTimeIdx).toString().toLong)
            val key = new SimpleDateFormat("yyyy-MM-dd HH:mm:00").format(rowTime)
            return new JTuple2(key, 1L)
          }
        }).returns(new TupleTypeInfo[JTuple2[String, Long]](
          BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO))
          .keyBy(0)
          .window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
          .sum(1).print()
      }
    })

    tableEnv.toAppendStream[Row](table, queryConfig).print()

    env.execute()
  }

  @SerialVersionUID(1L)
  class MyWatermarkExtractor extends AssignerWithPeriodicWatermarks[(Long, String, Int, Long)] {
    private var currentTimestamp = Long.MinValue

    override def extractTimestamp(element: (Long, String, Int, Long), previousElementTimestamp: Long): Long = {
      this.currentTimestamp = element._4
      element._4
    }

    override def getCurrentWatermark: Watermark = new Watermark(
      if (currentTimestamp == Long.MinValue) Long.MinValue
      else currentTimestamp - 1
    )
  }

}

object AllowedLatenessEventTimeTrigger {

  def of(): AllowedLatenessEventTimeTrigger = {
    new AllowedLatenessEventTimeTrigger()
  }
}

@SerialVersionUID(1L)
class AllowedLatenessEventTimeTrigger extends Trigger[CRow, Window] {

  final private val stateDesc = new ReducingStateDescriptor[JLong]("fire-time", new Min, LongSerializer.INSTANCE)

  /**
   * Called for every element that gets added to a pane. The result of this will determine
   * whether the pane is evaluated to emit results.
   *
   * @param element   The element that arrived.
   * @param timestamp The timestamp of the element that arrived.
   * @param window    The window to which the element is being added.
   * @param ctx       A context object that can be used to register timer callbacks.
   */
  @throws[Exception]
  override def onElement(element: CRow, timestamp: Long, window: Window, ctx: Trigger.TriggerContext): TriggerResult = {
    if (window.maxTimestamp <= ctx.getCurrentWatermark) {
      // 如果watermark已经越过了窗口，则注册一个1分钟基于ProcessingTime定时器，30秒后fire窗口
      val fireTimestamp = ctx.getPartitionedState(stateDesc)
      if (null != fireTimestamp.get()) {
        val timestamp = ctx.getCurrentProcessingTime
        val nextFireTimestamp = timestamp + 10 * 1000
        ctx.registerProcessingTimeTimer(nextFireTimestamp)
        fireTimestamp.add(nextFireTimestamp)
      }
      return TriggerResult.CONTINUE
    } else {
      ctx.registerEventTimeTimer(window.maxTimestamp())
      return TriggerResult.CONTINUE
    }
  }

  /**
   * Called when a processing-time timer that was set using the trigger context fires.
   *
   * @param time   The timestamp at which the timer fired.
   * @param window The window for which the timer fired.
   * @param ctx    A context object that can be used to register timer callbacks.
   */
  @throws[Exception]
  override def onProcessingTime(time: Long, window: Window, ctx: Trigger.TriggerContext): TriggerResult = {
    return TriggerResult.FIRE_AND_PURGE
  }

  /**
   * Called when an event-time timer that was set using the trigger context fires.
   *
   * @param time   The timestamp at which the timer fired.
   * @param window The window for which the timer fired.
   * @param ctx    A context object that can be used to register timer callbacks.
   */
  @throws[Exception]
  override def onEventTime(time: Long, window: Window, ctx: Trigger.TriggerContext): TriggerResult = {
    // fire 同时清理窗口state，使得延迟时间是增量的计算。
    if (time == window.maxTimestamp()) {
      return TriggerResult.FIRE_AND_PURGE
    } else if (time == window.maxTimestamp() + Time.hours(1).toMilliseconds) {
      val fireTimestamp = ctx.getPartitionedState(stateDesc)
      if (null != fireTimestamp.get()) {
        ctx.deleteProcessingTimeTimer(fireTimestamp.get())
        fireTimestamp.clear()
      }
      return TriggerResult.FIRE_AND_PURGE
    }
    return TriggerResult.CONTINUE
  }

  /**
   * Clears any state that the trigger might still hold for the given window. This is called
   * when a window is purged. Timers set using {@link TriggerContext#registerEventTimeTimer(long)}
   * and {@link TriggerContext#registerProcessingTimeTimer(long)} should be deleted here as
   * well as state acquired using {@link TriggerContext#getPartitionedState(StateDescriptor)}.
   */
  @throws[Exception]
  override def clear(window: Window, ctx: Trigger.TriggerContext): Unit = {
    val fireTimestamp = ctx.getPartitionedState(stateDesc)
    if (fireTimestamp.get() != null) {
      val timestamp = fireTimestamp.get()
      ctx.deleteProcessingTimeTimer(timestamp)
      fireTimestamp.clear()
    }
    ctx.deleteEventTimeTimer(window.maxTimestamp())
  }

  override def canMerge = true

  override def onMerge(window: Window, ctx: Trigger.OnMergeContext): Unit = {
    val windowMaxTimestamp = window.maxTimestamp()
    if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
      ctx.registerEventTimeTimer(windowMaxTimestamp)
    }
  }

  override def toString: String = "AllowedLatenessEventTimeTrigger()"

  @SerialVersionUID(1L)
  private class Min extends ReduceFunction[JLong] {
    @throws[Exception]
    override def reduce(value1: JLong, value2: JLong): JLong = Math.min(value1, value2)
  }
}
