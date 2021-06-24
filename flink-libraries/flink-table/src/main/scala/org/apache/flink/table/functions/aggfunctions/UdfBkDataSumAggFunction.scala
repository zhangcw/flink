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
package org.apache.flink.table.functions.aggfunctions

import java.lang.{Iterable => JIterable}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.AggregateFunction

/** The initial accumulator for long Sum aggregate function */
class LongSumAccumulator extends JTuple2[Long, Boolean] {
  f0 = 0L
  f1 = false
}

/**
  * Built-in long Sum aggregate function
  */
class LongBkDataSumAggFunction extends AggregateFunction[Long, LongSumAccumulator] {

  override def createAccumulator(): LongSumAccumulator = {
    new LongSumAccumulator
  }

  def accumulate(acc: LongSumAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Number].longValue
      acc.f0 = acc.f0 + v
      acc.f1 = true
    }
  }

  override def getValue(acc: LongSumAccumulator): Long = {
    if (acc.f1) {
      acc.f0
    } else {
      null.asInstanceOf[Long]
    }
  }

  def merge(acc: LongSumAccumulator, its: JIterable[LongSumAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f1) {
        acc.f0 = acc.f0 + a.f0
        acc.f1 = true
      }
    }
  }

  def resetAccumulator(acc: LongSumAccumulator): Unit = {
    acc.f0 = 0
    acc.f1 = false
  }

  override def getAccumulatorType: TypeInformation[LongSumAccumulator] = {
    new TupleTypeInfo(
      classOf[LongSumAccumulator],
      BasicTypeInfo.LONG_TYPE_INFO,
      BasicTypeInfo.BOOLEAN_TYPE_INFO)
  }

}
  //////////////////////////////////////////////////////

/** The initial accumulator for double Sum aggregate function */
class DoubleSumAccumulator extends JTuple2[Double, Boolean] {
  f0 = 0.0D
  f1 = false
}

/**
  * Built-in double Sum aggregate function
  */
class DoubleBkDataSumAggFunction extends AggregateFunction[Double, DoubleSumAccumulator] {

  override def createAccumulator(): DoubleSumAccumulator = {
    new DoubleSumAccumulator
  }

  def accumulate(acc: DoubleSumAccumulator, value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[Number].doubleValue
      acc.f0 = acc.f0 + v
      acc.f1 = true
    }
  }

  override def getValue(acc: DoubleSumAccumulator): Double = {
    if (acc.f1) {
      acc.f0
    } else {
      null.asInstanceOf[Double]
    }
  }

  def merge(acc: DoubleSumAccumulator, its: JIterable[DoubleSumAccumulator]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f1) {
        acc.f0 = acc.f0 + a.f0
        acc.f1 = true
      }
    }
  }

  def resetAccumulator(acc: DoubleSumAccumulator): Unit = {
    acc.f0 = 0.0D
    acc.f1 = false
  }

  override def getAccumulatorType: TypeInformation[DoubleSumAccumulator] = {
    new TupleTypeInfo(
      classOf[DoubleSumAccumulator],
      BasicTypeInfo.DOUBLE_TYPE_INFO,
      BasicTypeInfo.BOOLEAN_TYPE_INFO)
  }
}

