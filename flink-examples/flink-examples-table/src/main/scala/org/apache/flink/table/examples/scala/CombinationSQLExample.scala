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

import java.text.SimpleDateFormat
import java.lang.{Long => JLong}
import java.util.Date

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.ReducingStateDescriptor
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.table.api.{BkDataGroupWindowAggregateStreamQueryConfig, TableEnvironment}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row


/**
  * 混合驱动窗口【参考样例】
  */
object CombinationSQLExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dateFormat =new SimpleDateFormat("yyyy-MM-dd H:mm:ss")

    // read a DataStream from an external source
    val ds: DataStream[(Long, String, Int, Long)] = env
      .addSource(new SourceFunction[(Long, String, Int, Long)]() {
      def run(ctx: SourceFunction.SourceContext[(Long, String, Int, Long)]): Unit = {
        for (i <- 1 to 10) {
          ctx.collect((2L, "rubber", 1,
            dateFormat.parse("2019-01-01 00:00:00").getTime + i * 3*1000L))
          Thread.sleep(1000)
        }
        println("---------------------------------------")
        for (i <- 1 to 10) {
          println("source sleep:" + dateFormat.format(new Date()))
          Thread.sleep(10 * 1000)
        }
        println("source end." + dateFormat.format(new Date()))
      }

      def cancel() {}

    }).assignTimestampsAndWatermarks(new MyWatermarkExtractor())

    // register the DataStream under the name "Orders"
    tableEnv.registerDataStream("Orders", ds, 'user, 'product, 'amount, 'logtime,
      'proctime.proctime, 'rowtime.rowtime)

    // compute SUM(amount) per day (in event-time)
    val result1 = tableEnv.sqlQuery(
      """
        |SELECT
        |  user,
        |  TUMBLE_START(rowtime, INTERVAL '10' second) as wStart,
        |  SUM(amount)
        | FROM Orders
        | GROUP BY TUMBLE(rowtime, INTERVAL '10' second), user
      """.stripMargin)
    val queryConfig: BkDataGroupWindowAggregateStreamQueryConfig =
      new BkDataGroupWindowAggregateStreamQueryConfig
    // TODO： 指定一个自行扩展的出发器（可以实现混合驱动窗口，等）
    queryConfig.trigger(ProcessingTimeAndEventTimeTrigger.of(Time.seconds(10)))

    tableEnv.toAppendStream[Row](result1, queryConfig).print()

    // NonKeyedWindow 无key窗口
    val result2 = tableEnv.sqlQuery(
      """
        |SELECT
        |  'nonKey' as wType,
        |  TUMBLE_START(rowtime, INTERVAL '10' second) as wStart,
        |  SUM(amount)
        | FROM Orders
        | GROUP BY TUMBLE(rowtime, INTERVAL '10' second)
      """.stripMargin)
    val queryConfig2: BkDataGroupWindowAggregateStreamQueryConfig =
          new BkDataGroupWindowAggregateStreamQueryConfig
    // TODO： 指定一个自行扩展的出发器（可以实现混合驱动窗口，等）
    queryConfig2.trigger(ProcessingTimeAndEventTimeTrigger.of(Time.seconds(10)))
    tableEnv.toAppendStream[Row](result2, queryConfig2).print()

    env.execute
  }

  @SerialVersionUID(-1L)
  class MyWatermarkExtractor extends AssignerWithPeriodicWatermarks[(Long, String, Int, Long)] {
    private var currentTimestamp = Long.MinValue

    override def extractTimestamp(event: (Long, String, Int, Long),
                                  previousElementTimestamp: Long): Long = {
      this.currentTimestamp = event._4
      event._4
    }

    override def getCurrentWatermark = new Watermark(
      if (currentTimestamp == Long.MinValue) Long.MinValue
      else currentTimestamp - 1)
  }
}

object ProcessingTimeAndEventTimeTrigger {

  def of(processingTimeInterval: Time): ProcessingTimeAndEventTimeTrigger[Window] = {
    new ProcessingTimeAndEventTimeTrigger[Window](processingTimeInterval.toMilliseconds)
  }
}

/**
  * 混合驱动窗口
  * Copyright © 2012-2018 Tencent BlueKing.
  * All Rights Reserved.
  * 蓝鲸智云 版权所有
  */
@SerialVersionUID(1L)
class ProcessingTimeAndEventTimeTrigger[W <: Window] private(
               val processingTimeInterval: Long) extends Trigger[CRow, W] {
  // 基于processingTime的等待时间(如果达到窗口长度 + 等待时间了，watermark没有触发将有由系统触发)
  // 延迟数据窗口
  final private val latenessWindowLength = 0
  /** When merging we take the lowest of all fire timestamps as the new fire timestamp. */
  final private val stateDesc = new ReducingStateDescriptor[JLong]("fire-time", new Min, LongSerializer.INSTANCE)

  @throws[Exception]
  override def onElement(element: CRow, timestamp1: Long, window: W,
                         ctx: Trigger.TriggerContext): TriggerResult = {
    var timestamp = timestamp1
    if (window.maxTimestamp <= ctx.getCurrentWatermark) {
      if (latenessWindowLength > 0) {
        // 延迟数据注册一个1分钟的ProcessingTime窗口
        val fireTimestamp = ctx.getPartitionedState(stateDesc)
        if (fireTimestamp.get == null) {
          timestamp = ctx.getCurrentProcessingTime
          val nextFireTimestamp = timestamp + 60000
          ctx.registerProcessingTimeTimer(nextFireTimestamp)
          fireTimestamp.add(nextFireTimestamp)
          return TriggerResult.CONTINUE
        }
      } else {
        return TriggerResult.FIRE
      }
    } else {
      ctx.registerEventTimeTimer(window.maxTimestamp)
    }
    // 需要继续注册ProcessingTime
    val fireTimestamp = ctx.getPartitionedState(stateDesc)
    timestamp = ctx.getCurrentProcessingTime
    if (fireTimestamp.get == null && window.isInstanceOf[TimeWindow]) {
      val windowLength = window.asInstanceOf[TimeWindow].getEnd
                         - window.asInstanceOf[TimeWindow].getStart
      // 窗口被系统定时触发时间 = 当前系统时间 + 窗口长度 + 等待时间
      val nextFireTimestamp = timestamp + windowLength + processingTimeInterval
      ctx.registerProcessingTimeTimer(nextFireTimestamp)
      fireTimestamp.add(nextFireTimestamp)
      return TriggerResult.CONTINUE
    }
    TriggerResult.CONTINUE
  }

  @throws[Exception]
  override def onEventTime(time: Long, window: W, ctx: Trigger.TriggerContext): TriggerResult = {
    if (time == window.maxTimestamp) {
      //
      val fireTimestamp = ctx.getPartitionedState(stateDesc)
      if (fireTimestamp.get != null) {
        ctx.deleteProcessingTimeTimer(fireTimestamp.get)
        fireTimestamp.clear()
      }
      return TriggerResult.FIRE_AND_PURGE
    }
    TriggerResult.CONTINUE
  }

  @throws[Exception]
  override def onProcessingTime(time: Long, window: W,
                                ctx: Trigger.TriggerContext): TriggerResult = {
    val fireTimestamp = ctx.getPartitionedState(stateDesc)
    if (fireTimestamp.get == time) {
      ctx.deleteProcessingTimeTimer(time)
      fireTimestamp.clear()
      return TriggerResult.FIRE_AND_PURGE
    }
    TriggerResult.CONTINUE
  }

  @throws[Exception]
  override def clear(window: W, ctx: Trigger.TriggerContext): Unit = {
    val fireTimestamp = ctx.getPartitionedState(stateDesc)
    if (fireTimestamp.get != null) {
      val timestamp = fireTimestamp.get
      ctx.deleteProcessingTimeTimer(timestamp)
      fireTimestamp.clear()
    }
    ctx.deleteEventTimeTimer(window.maxTimestamp)
  }

  override def canMerge = true

  override def onMerge(window: W, ctx: Trigger.OnMergeContext): Unit = {
    ctx.mergePartitionedState(stateDesc)
  }

  @VisibleForTesting def getInterval: Long = processingTimeInterval

  override def toString: String =
                  "ProcessingTimeAndEventTimeTrigger(" + processingTimeInterval + ")"

  @SerialVersionUID(1L)
  private class Min extends ReduceFunction[JLong] {
    @throws[Exception]
    override def reduce(value1: JLong, value2: JLong): JLong = Math.min(value1, value2)
  }
}

