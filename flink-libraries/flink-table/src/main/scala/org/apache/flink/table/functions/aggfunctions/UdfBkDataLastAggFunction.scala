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
import java.math.BigDecimal
import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.tuple.{Tuple2 => JTuple2}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.AggregateFunction

/** The initial accumulator for bkdata_last aggregate function */
class UdfBkDataLastAccumulator[T] extends JTuple2[T, Boolean]

/**
  * Base class for built-in bkdata_last aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class UdfBkDataLastAggFunction[T]
  extends AggregateFunction[T, UdfBkDataLastAccumulator[T]] {

  override def createAccumulator(): UdfBkDataLastAccumulator[T] = {
    val acc = new UdfBkDataLastAccumulator[T]
    acc.f0 = getInitValue
    acc.f1 = false
    acc
  }

  def accumulate(acc: UdfBkDataLastAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      acc.f0 = v
      acc.f1 = true
    }
  }

  override def getValue(acc: UdfBkDataLastAccumulator[T]): T = {
    if (acc.f1) {
      acc.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: UdfBkDataLastAccumulator[T], its: JIterable[UdfBkDataLastAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f1) {
        accumulate(acc, a.f0)
      }
    }
  }

  def resetAccumulator(acc: UdfBkDataLastAccumulator[T]): Unit = {
    acc.f0 = getInitValue
    acc.f1 = false
  }

  override def getAccumulatorType: TypeInformation[UdfBkDataLastAccumulator[T]] = {
    new TupleTypeInfo(
      classOf[UdfBkDataLastAccumulator[T]],
      getValueTypeInfo,
      BasicTypeInfo.BOOLEAN_TYPE_INFO)
  }

  def getInitValue: T

  def getValueTypeInfo: TypeInformation[_]
}

/**
  * Built-in Byte bkdata_last aggregate function
  */
class ByteUdfBkDataLastAggFunction extends UdfBkDataLastAggFunction[Byte] {
  override def getInitValue: Byte = 0.toByte
  override def getValueTypeInfo = BasicTypeInfo.BYTE_TYPE_INFO
}

/**
  * Built-in Short bkdata_last aggregate function
  */
class ShortUdfBkDataLastAggFunction extends UdfBkDataLastAggFunction[Short] {
  override def getInitValue: Short = 0.toShort
  override def getValueTypeInfo = BasicTypeInfo.SHORT_TYPE_INFO
}

/**
  * Built-in Int bkdata_last aggregate function
  */
class IntUdfBkDataLastAggFunction extends UdfBkDataLastAggFunction[Int] {
  override def getInitValue: Int = 0
  override def getValueTypeInfo = BasicTypeInfo.INT_TYPE_INFO
}

/**
  * Built-in Long bkdata_last aggregate function
  */
class LongUdfBkDataLastAggFunction extends UdfBkDataLastAggFunction[Long] {
  override def getInitValue: Long = 0L
  override def getValueTypeInfo = BasicTypeInfo.LONG_TYPE_INFO
}

/**
  * Built-in Float bkdata_last aggregate function
  */
class FloatUdfBkDataLastAggFunction extends UdfBkDataLastAggFunction[Float] {
  override def getInitValue: Float = 0.0f
  override def getValueTypeInfo = BasicTypeInfo.FLOAT_TYPE_INFO
}

/**
  * Built-in Double bkdata_last aggregate function
  */
class DoubleUdfBkDataLastAggFunction extends UdfBkDataLastAggFunction[Double] {
  override def getInitValue: Double = 0.0d
  override def getValueTypeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO
}

/**
  * Built-in Boolean bkdata_last aggregate function
  */
class BooleanUdfBkDataLastAggFunction extends UdfBkDataLastAggFunction[Boolean] {
  override def getInitValue = false
  override def getValueTypeInfo = BasicTypeInfo.BOOLEAN_TYPE_INFO
}

/**
  * Built-in Big Decimal bkdata_last aggregate function
  */
class DecimalUdfBkDataLastAggFunction extends UdfBkDataLastAggFunction[BigDecimal] {
  override def getInitValue = BigDecimal.ZERO
  override def getValueTypeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO
}

/**
  * Built-in String bkdata_last aggregate function
  */
class StringUdfBkDataLastAggFunction extends UdfBkDataLastAggFunction[String] {
  override def getInitValue = ""
  override def getValueTypeInfo = BasicTypeInfo.STRING_TYPE_INFO
}

/**
  * Built-in Timestamp bkdata_last aggregate function
  */
class TimestampUdfBkDataLastAggFunction extends UdfBkDataLastAggFunction[Timestamp] {
  override def getInitValue: Timestamp = new Timestamp(0)
  override def getValueTypeInfo = Types.SQL_TIMESTAMP
}

/**
  * Built-in Date bkdata_last aggregate function
  */
class DateUdfBkDataLastAggFunction extends UdfBkDataLastAggFunction[Date] {
  override def getInitValue: Date = new Date(0)
  override def getValueTypeInfo = Types.SQL_DATE
}

/**
  * Built-in Time bkdata_last aggregate function
  */
class TimeUdfBkDataLastAggFunction extends UdfBkDataLastAggFunction[Time] {
  override def getInitValue: Time = new Time(0)
  override def getValueTypeInfo = Types.SQL_TIME
}
