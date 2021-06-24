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

package org.apache.flink.table.runtime.stream.sql;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class UdfBkdataFirstLastSqlTest extends AbstractTestBase {

	@Test
	public void testLastReturnTypeAndResult() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		List<Row> data = new ArrayList<>();
		data.add(Row.of(System.currentTimeMillis(), new Byte("1"), new Short("1"), 1, 1L, "1", true, new java.sql.Date(1548245633000L), new java.sql.Timestamp(1548245633000L)));
		data.add(Row.of(System.currentTimeMillis(), new Byte("2"), new Short("2"), 2, 2L, "2", true, new java.sql.Date(1548245633000L), new java.sql.Timestamp(1548245633000L)));
		data.add(Row.of(System.currentTimeMillis(), new Byte("3"), new Short("3"), 3, 3L, "3", true, new java.sql.Date(1548245634000L), new java.sql.Timestamp(1548245634000L)));

		TypeInformation<?>[] types = {
			BasicTypeInfo.LONG_TYPE_INFO, //eventTime
			BasicTypeInfo.BYTE_TYPE_INFO, //a
			BasicTypeInfo.SHORT_TYPE_INFO, //b
			BasicTypeInfo.INT_TYPE_INFO, //c
			BasicTypeInfo.LONG_TYPE_INFO, //d
			BasicTypeInfo.STRING_TYPE_INFO, //e
			BasicTypeInfo.BOOLEAN_TYPE_INFO, //f
			SqlTimeTypeInfo.DATE, //g
			SqlTimeTypeInfo.TIMESTAMP // h
		};
		String[] names = {"eventTime", "a", "b", "c", "d", "e", "f", "g", "h"};

		RowTypeInfo typeInfo = new RowTypeInfo(types, names);

		DataStream<Row> ds = env.fromCollection(data).returns(typeInfo).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {

			private static final long serialVersionUID = -1L;

			private long currentTimestamp = Long.MIN_VALUE;

			@Override
			public long extractTimestamp(Row element, long previousElementTimestamp) {
				this.currentTimestamp = (Long) element.getField(0);
				return (Long) element.getField(0);
			}

			@Override
			public Watermark getCurrentWatermark() {
				return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 10000);
			}
		});

		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e,f,g,h,rowtime.rowtime");
		tableEnv.registerTable("MyTableRow", in);

		String sqlQuery = "SELECT  " +
			"bkdata_last(a) as a, " +
			"bkdata_last(b) as b, " +
			"bkdata_last(c) as c, " +
			"bkdata_last(d) as d, " +
			"bkdata_last(e) as e, " +
			"bkdata_last(f) as f, " +
			"bkdata_last(g) as g, " +
			"bkdata_last(h) as h " +
			"FROM MyTableRow group by TUMBLE(rowtime, INTERVAL '1' DAY)";
		Table result = tableEnv.sqlQuery(sqlQuery);

		System.out.println(result.getSchema());
		for (int i = 0; i < result.getSchema().getColumnNames().length; i++) {
			String columnName = result.getSchema().getColumnNames()[i];
			System.out.println(columnName + "=" + result.getSchema().getFieldType(i).get());
			Assert.assertEquals(types[i + 1] , result.getSchema().getFieldType(i).get());
		}

		DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
		resultSet.print();
		resultSet.addSink(new StreamITCase.StringSink<Row>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("3,3,3,3,3,true,2019-01-23,2019-01-23 20:13:54.0");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testFirstReturnTypeAndResult() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		StreamITCase.clear();

		List<Row> data = new ArrayList<>();
		data.add(Row.of(System.currentTimeMillis(), new Byte("1"), new Short("1"), 1, 1L, "1", true, new java.sql.Date(1548245633000L), new java.sql.Timestamp(1548245633000L)));
		data.add(Row.of(System.currentTimeMillis(), new Byte("2"), new Short("2"), 2, 2L, "2", true, new java.sql.Date(1548245633000L), new java.sql.Timestamp(1548245633000L)));
		data.add(Row.of(System.currentTimeMillis(), new Byte("3"), new Short("3"), 3, 3L, "3", true, new java.sql.Date(1548245634000L), new java.sql.Timestamp(1548245634000L)));

		TypeInformation<?>[] types = {
			BasicTypeInfo.LONG_TYPE_INFO, //eventTime
			BasicTypeInfo.BYTE_TYPE_INFO, //a
			BasicTypeInfo.SHORT_TYPE_INFO, //b
			BasicTypeInfo.INT_TYPE_INFO, //c
			BasicTypeInfo.LONG_TYPE_INFO, //d
			BasicTypeInfo.STRING_TYPE_INFO, //e
			BasicTypeInfo.BOOLEAN_TYPE_INFO, //f
			SqlTimeTypeInfo.DATE, //g
			SqlTimeTypeInfo.TIMESTAMP // h
		};
		String[] names = {"eventTime", "a", "b", "c", "d", "e", "f", "g", "h"};

		RowTypeInfo typeInfo = new RowTypeInfo(types, names);

		DataStream<Row> ds = env.fromCollection(data).returns(typeInfo).assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {

			private static final long serialVersionUID = -1L;

			private long currentTimestamp = Long.MIN_VALUE;

			@Override
			public long extractTimestamp(Row element, long previousElementTimestamp) {
				this.currentTimestamp = (Long) element.getField(0);
				return (Long) element.getField(0);
			}

			@Override
			public Watermark getCurrentWatermark() {
				return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 10000);
			}
		});

		Table in = tableEnv.fromDataStream(ds, "a,b,c,d,e,f,g,h,rowtime.rowtime");
		tableEnv.registerTable("MyTableRow", in);

		String sqlQuery = "SELECT  " +
			"bkdata_first(a) as a, " +
			"bkdata_first(b) as b, " +
			"bkdata_first(c) as c, " +
			"bkdata_first(d) as d, " +
			"bkdata_first(e) as e, " +
			"bkdata_first(f) as f, " +
			"bkdata_first(g) as g, " +
			"bkdata_first(h) as h " +
			"FROM MyTableRow group by TUMBLE(rowtime, INTERVAL '1' DAY)";
		Table result = tableEnv.sqlQuery(sqlQuery);

		System.out.println(result.getSchema());
		for (int i = 0; i < result.getSchema().getColumnNames().length; i++) {
			String columnName = result.getSchema().getColumnNames()[i];
			System.out.println(columnName + "=" + result.getSchema().getFieldType(i).get());
			Assert.assertEquals(types[i + 1] , result.getSchema().getFieldType(i).get());
		}

		DataStream<Row> resultSet = tableEnv.toAppendStream(result, Row.class);
		resultSet.print();
		resultSet.addSink(new StreamITCase.StringSink<Row>());
		env.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,1,1,1,1,true,2019-01-23,2019-01-23 20:13:53.0");

		StreamITCase.compareWithList(expected);
	}
}
