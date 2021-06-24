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
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.BkDataGroupWindowAggregateStreamQueryConfig;
import org.apache.flink.table.api.BkDataSideOutputProcess;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.plan.schema.RowSchema;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;

public class AccumulativeSqlTest {
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
				public void run(SourceContext<Tuple4<Long, String, Integer, Long>> sourceContext) throws Exception {
					sourceContext.collect(new Tuple4<>(2222L, "rubber", 11, dateFormat.parse("2019-03-19 03:30:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 00:00:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 01:01:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 01:02:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 01:30:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 02:00:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 02:04:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 02:06:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 07:00:00").getTime()));

					System.out.println("To sleep 10 s");
					Thread.sleep(10 * 1000);

					// delay time
					sourceContext.collect(new Tuple4<>(2222L, "rubber", 11, dateFormat.parse("2019-03-19 02:30:00").getTime()));

					Thread.sleep(10 * 1000);

					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 08:00:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 09:00:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 10:00:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 11:00:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 12:00:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 13:00:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 14:00:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 18:00:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 18:00:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 18:00:00").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 18:00:30").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 18:00:30").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 18:00:31").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 3, dateFormat.parse("2019-03-20 23:01:30").getTime()));
					sourceContext.collect(new Tuple4<>(getId(), "rubber", 1, dateFormat.parse("2019-03-21 08:15:30").getTime()));

					Thread.sleep(300 * 1000);
				}

				@Override
				public void cancel() {

				}
			}).assignTimestampsAndWatermarks(new MyWatermarkExtractor());

		tEnv.registerDataStream("Orders", ds, "user, product, amount, logtime, rowtime.rowtime");

		Table table = tEnv.sqlQuery("\n" +
			"SELECT\n" +
			"  user,\n" +
			"  HOP_END(rowtime, INTERVAL '1' HOUR, INTERVAL '1' day) as wStart,\n" +
			"  SUM(amount)\n" +
			" FROM Orders\n" +
			" GROUP BY HOP(rowtime, INTERVAL '1' HOUR, INTERVAL '1' day), user");

		BkDataGroupWindowAggregateStreamQueryConfig streamQueryConfig = new BkDataGroupWindowAggregateStreamQueryConfig();
		// TODO：设置是否累加计算
		// streamQueryConfig.setSlidingToAccumulate(true);
		streamQueryConfig.setAccumulate(true);

		// TODO：设置时区偏移
		streamQueryConfig.setBkSqlWindowOffset(TimeZone.getTimeZone("Asia/Shanghai").getRawOffset());



		// 自定义允许延迟数据trigger
		streamQueryConfig.allowedLateness(Time.hours(48).toMilliseconds());
		streamQueryConfig.trigger(AllowedLatenessEventTimeTrigger.of(30, 48));

		// 设置输出延迟数据
		streamQueryConfig.sideOutputLateData("output tag test");
		streamQueryConfig.setSideOutputProcess(new BkDataSideOutputProcess() {
			private static final long serialVersionUID = 1L;

			@Override
			public void process(DataStream<CRow> dataStream, RowSchema inputSchema) {
				int rowTimeIdx = inputSchema.fieldNames().indexOf("rowtime");
				if (rowTimeIdx < 0) {
					throw new TableException("Time attribute could not be found. This is a bug.");
				}
				dataStream.map(new MapFunction<CRow, Tuple2<String, Long>>() {
					@Override
					public Tuple2<String, Long> map(CRow value) throws Exception {
						Date rowTime = new Date(Long.valueOf(value.row().getField(rowTimeIdx).toString()));
						String key = new SimpleDateFormat("yyyy-MM-dd HH:mm:00").format(rowTime);
						return new Tuple2<>(key, 1L);
					}
				}).returns(new TupleTypeInfo<>
					(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO))
					.keyBy(0)
					.window(TumblingProcessingTimeWindows.of(Time.seconds(60)))
					.sum(1).print();
			}
		});

		// TODO：设置自定义的查询配置
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

		env.execute("test-for-acc");

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
