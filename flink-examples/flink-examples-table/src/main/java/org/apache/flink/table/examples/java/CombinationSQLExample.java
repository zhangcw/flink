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

package org.apache.flink.table.examples.java;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.BkDataGroupWindowAggregateStreamQueryConfig;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.examples.scala.ProcessingTimeAndEventTimeTrigger;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * 混合窗口驱动.
 */
public class CombinationSQLExample {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 事件时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
		SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		DataStream<Tuple4<Long, String, Integer, Long>> ds = env
			.addSource(new SourceFunction<Tuple4<Long, String, Integer, Long>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public void run(SourceContext<Tuple4<Long, String, Integer, Long>> ctx) throws Exception {
					for (int i = 0; i <= 10; i++) {
						ctx.collect(new Tuple4<>(2L, "rubber", 1, dateFormat.parse("2019-01-01 00:00:00").getTime() + i * 3 * 1000L));
						Thread.sleep(1000);
					}
					System.out.println("-----------------------------------------");
					for (int i = 0; i < 10; i++) {
						System.out.println("source sleep:" + dateFormat.format(new Date()));
						Thread.sleep(10 * 1000);
					}
				}

				@Override
				public void cancel() {
				}
			})
			.assignTimestampsAndWatermarks(new MyWatermarkExtractor());

		tEnv.registerDataStream("Orders", ds, "user, product, amount, logtime, proctime.proctime, rowtime.rowtime");

		Table table = tEnv.sqlQuery("\n" +
			"SELECT\n" +
			"  user,\n" +
			"  TUMBLE_START(rowtime, INTERVAL '10' second) as wStart,\n" +
			"  SUM(amount)\n" +
			" FROM Orders\n" +
			" GROUP BY TUMBLE(rowtime, INTERVAL '10' second), user");

		BkDataGroupWindowAggregateStreamQueryConfig streamQueryConfig = new BkDataGroupWindowAggregateStreamQueryConfig();

		// TODO：》》自定义触发器实现混合窗口计算
		streamQueryConfig.trigger(ProcessingTimeAndEventTimeTrigger.of(Time.seconds(10)));

		DataStream<Row> result = tEnv.toAppendStream(table, Row.class, streamQueryConfig);
		result.print();

		env.execute("test-for-acc");

	}

	private static class MyWatermarkExtractor implements AssignerWithPeriodicWatermarks<Tuple4<Long, String, Integer, Long>> {

		private static final long serialVersionUID = 1L;

		private Long currentTimestamp = Long.MIN_VALUE;

		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			if (currentTimestamp == Long.MIN_VALUE) {
				return new Watermark(Long.MIN_VALUE);
			} else {
				return new Watermark(currentTimestamp - 1L);
			}
		}

		@Override
		public long extractTimestamp(Tuple4<Long, String, Integer, Long> element, long previousElementTimestamp) {
			this.currentTimestamp = element.f3;
			return this.currentTimestamp;
		}
	}
}
