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

/** The initial accumulator for bkdata_first aggregate function */
class UdfBkDataFirstAccumulator[T] extends JTuple2[T, Boolean]

/**
  * Base class for built-in bkdata_first aggregate function
  *
  * @tparam T the type for the aggregation result
  */
abstract class UdfBkDataFirstAggFunction[T]
  extends AggregateFunction[T, UdfBkDataFirstAccumulator[T]] {

  override def createAccumulator(): UdfBkDataFirstAccumulator[T] = {
    val acc = new UdfBkDataFirstAccumulator[T]
    acc.f0 = getInitValue
    acc.f1 = false
    acc
  }

  def accumulate(acc: UdfBkDataFirstAccumulator[T], value: Any): Unit = {
    if (value != null) {
      val v = value.asInstanceOf[T]
      if (!acc.f1) {
        acc.f0 = v
        acc.f1 = true
      }
    }
  }

  override def getValue(acc: UdfBkDataFirstAccumulator[T]): T = {
    if (acc.f1) {
      acc.f0
    } else {
      null.asInstanceOf[T]
    }
  }

  def merge(acc: UdfBkDataFirstAccumulator[T],
            its: JIterable[UdfBkDataFirstAccumulator[T]]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      if (a.f1) {
        accumulate(acc, a.f0)
      }
    }
  }

  def resetAccumulator(acc: UdfBkDataFirstAccumulator[T]): Unit = {
    acc.f0 = getInitValue
    acc.f1 = false
  }

  override def getAccumulatorType: TypeInformation[UdfBkDataFirstAccumulator[T]] = {
    new TupleTypeInfo(
      classOf[UdfBkDataFirstAccumulator[T]],
      getValueTypeInfo,
      BasicTypeInfo.BOOLEAN_TYPE_INFO)
  }

  def getInitValue: T

  def getValueTypeInfo: TypeInformation[_]
}

/**
  * Built-in Byte bkdata_first aggregate function
  */
class ByteUdfBkDataFirstAggFunction extends UdfBkDataFirstAggFunction[Byte] {
  override def getInitValue: Byte = 0.toByte
  override def getValueTypeInfo = BasicTypeInfo.BYTE_TYPE_INFO
}

/**
  * Built-in Short bkdata_first aggregate function
  */
class ShortUdfBkDataFirstAggFunction extends UdfBkDataFirstAggFunction[Short] {
  override def getInitValue: Short = 0.toShort
  override def getValueTypeInfo = BasicTypeInfo.SHORT_TYPE_INFO
}

/**
  * Built-in Int bkdata_first aggregate function
  */
class IntUdfBkDataFirstAggFunction extends UdfBkDataFirstAggFunction[Int] {
  override def getInitValue: Int = 0
  override def getValueTypeInfo = BasicTypeInfo.INT_TYPE_INFO
}

/**
  * Built-in Long bkdata_first aggregate function
  */
class LongUdfBkDataFirstAggFunction extends UdfBkDataFirstAggFunction[Long] {
  override def getInitValue: Long = 0L
  override def getValueTypeInfo = BasicTypeInfo.LONG_TYPE_INFO
}

/**
  * Built-in Float bkdata_first aggregate function
  */
class FloatUdfBkDataFirstAggFunction extends UdfBkDataFirstAggFunction[Float] {
  override def getInitValue: Float = 0.0f
  override def getValueTypeInfo = BasicTypeInfo.FLOAT_TYPE_INFO
}

/**
  * Built-in Double bkdata_first aggregate function
  */
class DoubleUdfBkDataFirstAggFunction extends UdfBkDataFirstAggFunction[Double] {
  override def getInitValue: Double = 0.0d
  override def getValueTypeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO
}

/**
  * Built-in Boolean bkdata_first aggregate function
  */
class BooleanUdfBkDataFirstAggFunction extends UdfBkDataFirstAggFunction[Boolean] {
  override def getInitValue = false
  override def getValueTypeInfo = BasicTypeInfo.BOOLEAN_TYPE_INFO
}

/**
  * Built-in Big Decimal bkdata_first aggregate function
  */
class DecimalUdfBkDataFirstAggFunction extends UdfBkDataFirstAggFunction[BigDecimal] {
  override def getInitValue = BigDecimal.ZERO
  override def getValueTypeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO
}

/**
  * Built-in String bkdata_first aggregate function
  */
class StringUdfBkDataFirstAggFunction extends UdfBkDataFirstAggFunction[String] {
  override def getInitValue = ""
  override def getValueTypeInfo = BasicTypeInfo.STRING_TYPE_INFO
}

/**
  * Built-in Timestamp bkdata_first aggregate function
  */
class TimestampUdfBkDataFirstAggFunction extends UdfBkDataFirstAggFunction[Timestamp] {
  override def getInitValue: Timestamp = new Timestamp(0)
  override def getValueTypeInfo = Types.SQL_TIMESTAMP
}

/**
  * Built-in Date bkdata_first aggregate function
  */
class DateUdfBkDataFirstAggFunction extends UdfBkDataFirstAggFunction[Date] {
  override def getInitValue: Date = new Date(0)
  override def getValueTypeInfo = Types.SQL_DATE
}

/**
  * Built-in Time bkdata_first aggregate function
  */
class TimeUdfBkDataFirstAggFunction extends UdfBkDataFirstAggFunction[Time] {
  override def getInitValue: Time = new Time(0)
  override def getValueTypeInfo = Types.SQL_TIME
}
