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

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.types.Row;

public class SlideWindowTest {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// 事件时间
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		StreamTableEnvironment tEnv = TableEnvironment.getTableEnvironment(env);

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		dateFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
		SimpleDateFormat utcFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		utcFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

		DataStream<Tuple4<Long, String, Integer, Long>> ds = env
			.addSource(new SourceFunction<Tuple4<Long, String, Integer, Long>>() {
				@Override
				public void run(SourceContext<Tuple4<Long, String, Integer, Long>> ctx) throws Exception {
					ctx.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:10:00").getTime()));
					ctx.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:20:10").getTime()));
					ctx.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:20:11").getTime()));
					ctx.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:30:10").getTime()));

					Thread.sleep(10 *1000);
					ctx.collect(new Tuple4<>(getId(), "rubber", 4, dateFormat.parse("2019-03-20 00:10:10").getTime()));

					ctx.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:30:12").getTime()));
					ctx.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:50:10").getTime()));
				}

				@Override
				public void cancel() {

				}
			}).assignTimestampsAndWatermarks(new MyWatermarkExtractor());

		BkDataGroupWindowAggregateStreamQueryConfig streamQueryConfig = new BkDataGroupWindowAggregateStreamQueryConfig();

		// 自定义允许延迟数据trigger
		streamQueryConfig.allowedLateness(Time.hours(48).toMilliseconds());
		streamQueryConfig.trigger(AllowedLatenessEventTimeTrigger.of(30, 48));

		tEnv.registerDataStream("Orders", ds, "user, product, amount, logtime, proctime.proctime, rowtime.rowtime");

		Table table = tEnv.sqlQuery("\n" +
			"SELECT\n" +
			"  user,\n" +
			"  HOP_START(rowtime, INTERVAL '3' minute, INTERVAL '10' MINUTE) as wStart,\n" +
			"  HOP_END(rowtime, INTERVAL '3' minute, INTERVAL '10' MINUTE) as wStart,\n" +
			"  SUM(amount)\n" +
			" FROM Orders\n" +
			" GROUP BY HOP(rowtime, INTERVAL '3' minute, INTERVAL '10' MINUTE), user");

		DataStream<Row> result = tEnv.toAppendStream(table, Row.class, streamQueryConfig);

		result.map(new MapFunction<Row, Row>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Row map(Row value) throws Exception {
				// UTC时间转当前时区显示
				String dt = dateFormat.format(utcFormat.parse(value.getField(1).toString()));
				value.setField(1, dt);
				return value;
			}
		}).print();

//		ds.keyBy(new KeySelector<Tuple4<Long, String, Integer, Long>, Long>() {
//			@Override
//			public Long getKey(Tuple4<Long, String, Integer, Long> value) throws Exception {
//				return value.f0;
//			}})
//			.window(SlidingEventTimeWindows.of(Time.minutes(10), Time.seconds(180)))
//			.allowedLateness(Time.minutes(30))
//			.sum(2)
//			.print();

		env.execute("test for slide");
	}

	private static AtomicLong id = new AtomicLong(10000);
	private static long getId(){
		return id.get();
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
				return new Watermark(currentTimestamp - 30 * 1000L);
			}
		}

		@Override
		public long extractTimestamp(Tuple4<Long, String, Integer, Long> element, long previousElementTimestamp) {
			this.currentTimestamp = element.f3;
			return this.currentTimestamp;
		}
	}
}
