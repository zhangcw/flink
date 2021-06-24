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
import java.util.Date

import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.{TimeCharacteristic, datastream}
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row


/**
  * 延迟数据处理【参考样例】.
  */
object LatenessSideOutPutSQLExample {

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
        ctx.collect((2L, "rubber", 1, dateFormat.parse("2019-01-01 10:00:00").getTime))
        for (i <- 1 to 100) {
          ctx.collect((2L, "rubber", 1,
            dateFormat.parse("2019-01-01 00:00:00").getTime - i * 30*1000L))
          Thread.sleep(10*1000)
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
        |  COUNT(product)
        | FROM Orders
        | GROUP BY TUMBLE(rowtime, INTERVAL '10' second), user
      """.stripMargin)
    // 使用扩展的窗口查询配置器
    /**
      * 使用扩展的窗口查询配置器，支持如下功能：
      * 1、自定义出发器
      * 2、allowedLateness：设置允许延迟数据时间，在指定延迟时间内到达的数据还可以进行计算。
      * 3、sideOutputLateData + setSideOutputProcess：设置延迟数据侧输出，可以指定对延迟数据的处理。
      *
      */
    val queryConfig: BkDataGroupWindowAggregateStreamQueryConfig = new BkDataGroupWindowAggregateStreamQueryConfig

    // TODO: 重要
    // 1、设置侧输出延迟数据输出标签
    queryConfig.sideOutputLateData("OutputTag-123")
    // 2、设置侧输出延迟数据的处理器
    queryConfig.setSideOutputProcess(new BkDataSideOutputProcess {
      override def process(dataStream: datastream.DataStream[CRow],
                           inputSchema: RowSchema): Unit = {
        val rowTimeIdx = inputSchema.fieldNames.indexOf("rowtime")
        if (rowTimeIdx < 0) {
          throw new TableException("Time attribute could not be found. This is a bug.")
        }
        dataStream.map(new MapFunction[CRow, JTuple2[String, Long]] {
          override def map(value: CRow): JTuple2[String, Long] = {
            val key = new SimpleDateFormat("yyyy-MM-dd HH:mm:00")
                      .format(value.row.getField(rowTimeIdx).toString.toLong)
            new JTuple2[String, Long](key, 1L)
          }
        }).returns(new TupleTypeInfo[JTuple2[String, Long]](BasicTypeInfo.STRING_TYPE_INFO,
          BasicTypeInfo.LONG_TYPE_INFO))
          .keyBy(0).sum(1).print()
      }
    })

    tableEnv.toAppendStream[Row](result1, queryConfig)

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

