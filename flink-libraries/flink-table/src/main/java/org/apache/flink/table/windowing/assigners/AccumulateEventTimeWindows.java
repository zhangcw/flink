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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class AccumulateEventTimeWindows extends WindowAssigner<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	private final long size;

	private final long slide;

	/**
	 * 时区偏移.
	 */
	private final long offset;

	protected AccumulateEventTimeWindows(long size, long slide, long offset) {
		if (Math.abs(offset) > Time.hours(12).toMilliseconds() || size <= 0) {
			throw new IllegalArgumentException("AccumulateEventTimeWindows parameters must satisfy |offset| < 12 * 3600 * 1000 and windowSize > 0");
		}

		this.size = size;
		this.slide = slide;
		this.offset = offset;
	}

	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		if (timestamp > Long.MIN_VALUE) {
			List<TimeWindow> windows = new ArrayList<>((int) (size / slide));
			long windowStart = TimeWindow.getWindowStartWithOffset(timestamp, -offset, size);
			for (int i = (int) ((timestamp - windowStart) / slide) + 1; i <= (int) (size / slide); i++) {
				windows.add(new TimeWindow(windowStart, windowStart + i * slide));
			}
			return windows;
		} else {
			throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
				"Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
				"'DataStream.assignTimestampsAndWatermarks(...)'?");
		}
	}

	@Override
	public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
		return EventTimeTrigger.create();
	}

	public static AccumulateEventTimeWindows of(Time size, Time slide, Time offset) {
		return new AccumulateEventTimeWindows(size.toMilliseconds(), slide.toMilliseconds(), offset.toMilliseconds());
	}

	@Override
	public String toString() {
		return "AccumulateEventTimeWindows(" + size + ", " + slide + ", " + offset + ")";
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new TimeWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return true;
	}
}
