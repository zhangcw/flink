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

package org.apache.flink.table.windowing.assigners;

import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.TimeZone;

import static org.apache.flink.streaming.runtime.operators.windowing.StreamRecordMatchers.timeWindow;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class AccumulateEventTimeWindowsTest extends TestLogger {

	@Test
	public void testWindowAssignment() {
		WindowAssigner.WindowAssignerContext mockContext =
			mock(WindowAssigner.WindowAssignerContext.class);

		AccumulateEventTimeWindows assigner =
			AccumulateEventTimeWindows.of(Time.days(1), Time.hours(1), Time.milliseconds(TimeZone.getTimeZone("Asia/Shanghai").getRawOffset()));

		assertThat(assigner.assignWindows("String", 1576811870000L, mockContext), containsInAnyOrder(
			timeWindow(1576771200000L, 1576814400000L),
			timeWindow(1576771200000L, 1576818000000L),
			timeWindow(1576771200000L, 1576821600000L),
			timeWindow(1576771200000L, 1576825200000L),
			timeWindow(1576771200000L, 1576828800000L),
			timeWindow(1576771200000L, 1576832400000L),
			timeWindow(1576771200000L, 1576836000000L),
			timeWindow(1576771200000L, 1576839600000L),
			timeWindow(1576771200000L, 1576843200000L),
			timeWindow(1576771200000L, 1576846800000L),
			timeWindow(1576771200000L, 1576850400000L),
			timeWindow(1576771200000L, 1576854000000L),
			timeWindow(1576771200000L, 1576857600000L)
		));
	}
}

