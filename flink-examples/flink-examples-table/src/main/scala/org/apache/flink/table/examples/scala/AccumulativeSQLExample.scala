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
import java.util.TimeZone

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{BkDataGroupWindowAggregateStreamQueryConfig, TableEnvironment}
import org.apache.flink.types.Row

/**
  * 累加窗口Example类【参考样例】
  */
object AccumulativeSQLExample {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val tableEnv = TableEnvironment.getTableEnvironment(env)

    val dateFormat =new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    val utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    // 时间切换时区显示
    val utcToDataFormat = (t: Any) => {dateFormat.format(utcFormat.parse(t.toString).getTime)}

    val ds: DataStream[(Long, String, Int, Long)] = env
      .fromCollection(Seq(
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 00:00:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 01:01:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 01:02:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 01:30:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 02:00:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 02:04:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 02:06:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 07:00:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 08:00:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 09:00:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 10:00:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 11:00:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 12:00:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 13:00:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 14:00:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 18:00:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 18:00:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 18:00:00").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 18:00:30").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 18:00:30").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 18:00:31").getTime),
        (2L, "rubber", 3, dateFormat.parse("2019-03-20 23:01:30").getTime),
        (2L, "rubber", 1, dateFormat.parse("2019-03-21 08:15:30").getTime)
      )).assignTimestampsAndWatermarks(new MyWatermarkExtractor())

    // register the DataStream under the name "Orders"
    tableEnv.registerDataStream("Orders", ds, 'user, 'product, 'amount, 'logtime,
      'proctime.proctime, 'rowtime.rowtime)

    // compute SUM(amount) per day (in event-time)
    val result1 = tableEnv.sqlQuery(
      """
        |SELECT
        |  user,
        |  HOP_START(rowtime, INTERVAL '10' MINUTE, INTERVAL '1' day) as wStart,
        |  SUM(amount)
        | FROM Orders
        | GROUP BY HOP(rowtime, INTERVAL '10' MINUTE, INTERVAL '1' day), user
      """.stripMargin)

    val queryConfig: BkDataGroupWindowAggregateStreamQueryConfig =
      new BkDataGroupWindowAggregateStreamQueryConfig
    // 与UTC时间的差
    queryConfig.setBkSqlWindowOffset(TimeZone.getTimeZone("Asia/Shanghai").getRawOffset)
    // 指定滑动窗口需要使用累加窗口（没有修改协议，通过queryConfig设置）
    queryConfig.setSlidingToAccumulate(true)
    tableEnv.toAppendStream[Row](result1, queryConfig).map(x=>{
      ( x.getField(0),
        utcToDataFormat(x.getField(1)),
        x.getField(2)
      )
    }).print()
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
      else currentTimestamp - 30*1000)
  }
}
