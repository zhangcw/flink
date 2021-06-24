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

package org.apache.flink.table.api

import _root_.java.io.Serializable

import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.table.plan.schema.RowSchema
import org.apache.flink.table.runtime.types.CRow

class QueryConfig private[table] extends Serializable {}

/**
  * The [[BatchQueryConfig]] holds parameters to configure the behavior of batch queries.
  */
class BatchQueryConfig private[table] extends QueryConfig

/**
  * The [[StreamQueryConfig]] holds parameters to configure the behavior of streaming queries.
  *
  * An empty [[StreamQueryConfig]] can be generated using the [[StreamTableEnvironment.queryConfig]]
  * method.
  */
class StreamQueryConfig private[table] extends QueryConfig {

  /**
    * The minimum time until state which was not updated will be retained.
    * State might be cleared and removed if it was not updated for the defined period of time.
    */
  private var minIdleStateRetentionTime: Long = 0L

  /**
    * The maximum time until state which was not updated will be retained.
    * State will be cleared and removed if it was not updated for the defined period of time.
    */
  private var maxIdleStateRetentionTime: Long = 0L

  /**
    * Specifies a minimum and a maximum time interval for how long idle state, i.e., state which
    * was not updated, will be retained.
    * State will never be cleared until it was idle for less than the minimum time and will never
    * be kept if it was idle for more than the maximum time.
    *
    * When new data arrives for previously cleaned-up state, the new data will be handled as if it
    * was the first data. This can result in previous results being overwritten.
    *
    * Set to 0 (zero) to never clean-up the state.
    *
    * NOTE: Cleaning up state requires additional bookkeeping which becomes less expensive for
    * larger differences of minTime and maxTime. The difference between minTime and maxTime must be
    * at least 5 minutes.
    *
    * @param minTime The minimum time interval for which idle state is retained. Set to 0 (zero) to
    *                never clean-up the state.
    * @param maxTime The maximum time interval for which idle state is retained. Must be at least
    *                5 minutes greater than minTime. Set to 0 (zero) to never clean-up the state.
    */
  def withIdleStateRetentionTime(minTime: Time, maxTime: Time): StreamQueryConfig = {

    if (maxTime.toMilliseconds - minTime.toMilliseconds < 300000 &&
      !(maxTime.toMilliseconds == 0 && minTime.toMilliseconds == 0)) {
      throw new IllegalArgumentException(
        s"Difference between minTime: ${minTime.toString} and maxTime: ${maxTime.toString} " +
          s"shoud be at least 5 minutes.")
    }
    minIdleStateRetentionTime = minTime.toMilliseconds
    maxIdleStateRetentionTime = maxTime.toMilliseconds
    this
  }

  def getMinIdleStateRetentionTime: Long = {
    minIdleStateRetentionTime
  }

  def getMaxIdleStateRetentionTime: Long = {
    maxIdleStateRetentionTime
  }
}

/**
  * 扩展的SQL查询配置
  * 1、支持自定义出发器。
  * 2、支持设置允许数据延迟事件（延迟时间内的数据可以进行计算，但会有多条记录）
  * 3、支持配置延迟数据侧输出
  * 4、支持设置滑动窗口为累加窗口
  */
class BkDataGroupWindowAggregateStreamQueryConfig extends StreamQueryConfig {
  import org.apache.flink.streaming.api.windowing.windows.{Window => DataStreamWindow}

  private var trigger: Trigger[_ >: CRow, _ >: DataStreamWindow] = null
  private var allowedLateness = 0L
  private var lateDataOutputTagId: String = null
  private var process: BkDataSideOutputProcess = null

  /**
    * 滑动转累加窗口偏移（按天的时候需要根据UTC时间设置相应的偏移）
    */
  private var bkSqlWindowOffset: Long = 0L

  /**
    * 是否滑动转累加窗口
    */
  private var slidingToAccumulate: Boolean = false

  /**
   * 是否累加窗口 20191220
   */
  private var isAccumulate: Boolean = false

  /**
    * 是否累加窗口
    * @return
    */
  def isSlidingToAccumulate: Boolean = slidingToAccumulate

  /**
   * 是否累加窗口 20191220
   * @return
   */
  def isAccumulateWindow: Boolean = isAccumulate

  def getBkSqlWindowOffset: Long = {
    bkSqlWindowOffset
  }

  def setBkSqlWindowOffset(bkSqlWindowOffset: Long): Unit = {
    this.bkSqlWindowOffset = bkSqlWindowOffset
  }

  def setSlidingToAccumulate(slidingToAccumulate: Boolean): Unit = {
    this.slidingToAccumulate = slidingToAccumulate
  }

  /**
   * 设置累加窗口 20191220
   * @param isAccumulate default false
   */
  def setAccumulate(isAccumulate: Boolean): Unit = {
    this.isAccumulate = isAccumulate
  }

  def getTrigger: Trigger[_ >: CRow, _ >: DataStreamWindow]= trigger
  def getAllowedLateness: Long = allowedLateness
  def getLateDataOutputTagId: String = lateDataOutputTagId
  def getProcess: BkDataSideOutputProcess = process

  def trigger(trigger: Trigger[_ >: CRow, _ >: DataStreamWindow] ):
          BkDataGroupWindowAggregateStreamQueryConfig ={
    this.trigger = trigger
    this
  }
  def allowedLateness(allowedLateness: Long):
          BkDataGroupWindowAggregateStreamQueryConfig ={
    this.allowedLateness = allowedLateness
    this
  }
  def sideOutputLateData(lateDataOutputTagId: String):
          BkDataGroupWindowAggregateStreamQueryConfig ={
    this.lateDataOutputTagId = lateDataOutputTagId
    this
  }

  def setSideOutputProcess(process: BkDataSideOutputProcess):
          BkDataGroupWindowAggregateStreamQueryConfig ={
    this.process = process
    this
  }

}

/**
  * 延迟数据侧输出处理器
  */
trait BkDataSideOutputProcess extends Serializable {
  def process(dataStream: DataStream[CRow], inputSchema: RowSchema): Unit
}
