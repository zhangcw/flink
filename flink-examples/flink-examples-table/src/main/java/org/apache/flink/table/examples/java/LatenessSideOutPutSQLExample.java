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

import javax.annotation.Nullable;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * 延迟数据输了器【参考样例】
 * 这里统计每分钟延迟数据量.
 */
public class LatenessSideOutPutSQLExample {

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// env.setParallelism(1);
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
					ctx.collect(new Tuple4<>(2L, "rubber", 1, dateFormat.parse("2019-01-01 10:00:00").getTime()));
					for (int i = 0; i <= 10; i++) {
						ctx.collect(new Tuple4<>(2L, "rubber", 1, dateFormat.parse("2019-01-01 00:00:00").getTime() - i * 30 * 1000L));
						Thread.sleep(1000);
					}
					System.out.println("-----------------------------------------");
					Thread.sleep(2 * 60 * 1000);
					System.out.println("source end:" + dateFormat.format(new Date()));
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

		/**
		 * 使用扩展的窗口查询配置器，支持如下功能：
		 * 1、自定义出发器
		 * 2、allowedLateness：设置允许延迟数据时间，在指定延迟时间内到达的数据还可以进行计算。
		 * 3、sideOutputLateData + setSideOutputProcess：设置延迟数据侧输出，可以指定对延迟数据的处理。
		 *
		 */
		BkDataGroupWindowAggregateStreamQueryConfig queryConfig = new BkDataGroupWindowAggregateStreamQueryConfig();

		// TODO: 重要
		// 1、设置侧输出延迟数据输出标签
		queryConfig.sideOutputLateData("OutputTag-123");
		// 2、设置侧输出延迟数据的处理器
		queryConfig.setSideOutputProcess(new BkDataSideOutputProcess() {
			private static final long serialVersionUID = 1L;
			@Override
			public void process(DataStream<CRow> dataStream, RowSchema inputSchema) {
				int rowTimeIdx = inputSchema.fieldNames().indexOf("rowtime");
				if (rowTimeIdx < 0) {
					throw new TableException("Time attribute could not be found. This is a bug.");
				}
				dataStream.map(new MapFunction<CRow, Tuple2<String, Long>>() {
					private static final long serialVersionUID = 1L;
					@Override
					public Tuple2<String, Long> map (CRow value){
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

		tEnv.toAppendStream(table, Row.class, queryConfig);

		env.execute("test-for-sideout");
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
