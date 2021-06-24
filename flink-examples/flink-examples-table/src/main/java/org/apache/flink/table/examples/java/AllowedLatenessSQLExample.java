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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
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
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.api.BkDataGroupWindowAggregateStreamQueryConfig;
import org.apache.flink.table.api.BkDataSideOutputProcess;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.plan.schema.RowSchema;
import org.apache.flink.table.runtime.types.CRow;
import org.apache.flink.types.Row;

public class AllowedLatenessSQLExample {

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

				private long initTime = System.currentTimeMillis();

				private int c = 0;

				@Override
				public void run(SourceContext<Tuple4<Long, String, Integer, Long>> ctx) throws Exception {
					for (int i = 0; i <= 62; i++) {
						ctx.collect(new Tuple4<>(2L, "rubber", 1, dateFormat.parse("2019-01-01 00:00:00").getTime() + i * 30 * 1000L));
						Thread.sleep(100);
					}
					System.out.println("--delay data-----");
					// 延迟的数据
					for (int i = 0; i <= 600; i++) {
						ctx.collect(new Tuple4<>(3L, "rubber", 1, dateFormat.parse("2019-01-01 00:00:00").getTime() + i * 30 * 1000L));
						Thread.sleep(100);
						if (System.currentTimeMillis() - initTime >= 10 * 1000) {
							System.out.println("====================10 second====================");
							initTime = System.currentTimeMillis();
							// 特定延迟数据
							ctx.collect(new Tuple4<>(4L, "rubber", 1, utcFormat.parse("2018-12-31 16:39:20").getTime() + 300 * 1000L * c));
							System.out.println(new Tuple4<>(4L, "rubber", 1, utcFormat.format(new Date(utcFormat.parse("2018-12-31 16:39:20").getTime() + 300 * 1000L * c))));
							c ++;
						}
					}
					System.out.println("-----------------------------------------");
					for (int i = 0; i < 100; i++) {
						Thread.sleep( 1000);
					}
				}

				@Override
				public void cancel() {
				}
			})
			.assignTimestampsAndWatermarks(new MyWatermarkExtractor());

		tEnv.registerDataStream("Orders", ds, "user, product, amount, logtime, rowtime.rowtime");

		Table table = tEnv.sqlQuery("\n" +
			"SELECT\n" +
			"  user,\n" +
			"  TUMBLE_START(rowtime, INTERVAL '5' minute) as wStart,\n" +
			"  SUM(amount)\n" +
			" FROM Orders\n" +
			" GROUP BY TUMBLE(rowtime, INTERVAL '5' minute), user");

		// 使用扩展的查询配置，可以设置自定义的trigger触发器。
		BkDataGroupWindowAggregateStreamQueryConfig streamQueryConfig = new BkDataGroupWindowAggregateStreamQueryConfig();

		// TODO：自定义延迟1小时内的数据，按处理时间分钟聚合后输出一个增量值
		streamQueryConfig.allowedLateness(Time.hours(1).toMilliseconds());
		streamQueryConfig.trigger(AllowedLatenessEventTimeTrigger.create());

		// 设置侧输出延迟数据输出标签
		streamQueryConfig.sideOutputLateData("OutputTag-test");
		// 设置侧输出延迟数据的处理器
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

		DataStream<Row> result = tEnv.toAppendStream(table, Row.class, streamQueryConfig);
		result.print();

		env.execute("test-for-combination");

	}


	/**
	 * 定义一个延迟数据支持1分钟增量计算触发器.
	 */
	public static class AllowedLatenessEventTimeTrigger extends Trigger<CRow, Window> {
		private static final long serialVersionUID = 1L;

		private AllowedLatenessEventTimeTrigger() {}

		/** When merging we take the lowest of all fire timestamps as the new fire timestamp. */
		private final ReducingStateDescriptor<Long> stateDesc =
			new ReducingStateDescriptor<>("fire-time-AllowedLateness", new Min(), LongSerializer.INSTANCE);

		@Override
		public TriggerResult onElement(CRow element, long timestamp, Window window, TriggerContext ctx) throws Exception {
			if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {

				// 如果watermark已经越过了窗口，则注册一个1分钟基于ProcessingTime定时器，30秒后fire窗口
				ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
				if (fireTimestamp.get() == null) {
					timestamp = ctx.getCurrentProcessingTime();
					long nextFireTimestamp = timestamp + 10 * 1000;
					ctx.registerProcessingTimeTimer(nextFireTimestamp);
					fireTimestamp.add(nextFireTimestamp);
				}
				return TriggerResult.CONTINUE;
			} else {
				ctx.registerEventTimeTimer(window.maxTimestamp());
				return TriggerResult.CONTINUE;
			}
		}

		@Override
		public TriggerResult onEventTime(long time, Window window, TriggerContext ctx)throws Exception {
			// fire 同时清理窗口state，使得延迟时间是增量的计算。
			if (time == window.maxTimestamp()) {
				return TriggerResult.FIRE_AND_PURGE;
			} else if (time == window.maxTimestamp() + Time.hours(1).toMilliseconds()) {
				ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
				if (fireTimestamp.get() != null) {
					ctx.deleteProcessingTimeTimer(fireTimestamp.get());
					fireTimestamp.clear();
				}
				return TriggerResult.FIRE_AND_PURGE;
			}
			return TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
			return TriggerResult.FIRE_AND_PURGE;
		}

		@Override
		public void clear(Window window, TriggerContext ctx) throws Exception {
			ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
			if (fireTimestamp.get() != null) {
				long timestamp = fireTimestamp.get();
				ctx.deleteProcessingTimeTimer(timestamp);
				fireTimestamp.clear();
			}
			ctx.deleteEventTimeTimer(window.maxTimestamp());
		}

		@Override
		public boolean canMerge() {
			return true;
		}

		@Override
		public void onMerge(Window window,
							OnMergeContext ctx) {
			// only register a timer if the watermark is not yet past the end of the merged window
			// this is in line with the logic in onElement(). If the watermark is past the end of
			// the window onElement() will fire and setting a timer here would fire the window twice.
			long windowMaxTimestamp = window.maxTimestamp();
			if (windowMaxTimestamp > ctx.getCurrentWatermark()) {
				ctx.registerEventTimeTimer(windowMaxTimestamp);
			}
		}

		@Override
		public String toString() {
			return "AllowedLatenessEventTimeTrigger()";
		}

		/**
		 * Creates an event-time trigger that fires once the watermark passes the end of the window.
		 *
		 * <p>Once the trigger fires all elements are discarded. Elements that arrive late immediately
		 * trigger window evaluation with just this one element.
		 */
		public static AllowedLatenessEventTimeTrigger create() {
			return new AllowedLatenessEventTimeTrigger();
		}

		private static class Min implements ReduceFunction<Long> {
			private static final long serialVersionUID = 1L;

			@Override
			public Long reduce(Long value1, Long value2) throws Exception {
				return Math.min(value1, value2);
			}
		}
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
			this.currentTimestamp = Math.max(this.currentTimestamp, element.f3);
			// 返回记录的时间
			return element.f3;
		}
	}
}
