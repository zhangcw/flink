/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.windowing.assigners;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 基于事件事件的累加计算,窗口分配.
 */
public class EventTimeAccumulateWindows extends MergingWindowAssigner<Object, TimeWindow> {
	private static final long serialVersionUID = 1L;

	private final long windowSize;

	private final long slide;

	private final long offset;

	private EventTimeAccumulateWindows(long windowSize, long slide, long offset) {
		if (Math.abs(offset) > Time.hours(12).toMilliseconds() || windowSize <= 0) {
			throw new IllegalArgumentException("EventTimeAccumulateWindows parameters must satisfy |offset| < 12*3600*1000 and windowSize > 0");
		}

		this.windowSize = windowSize;
		this.slide = slide;
		this.offset = offset;
	}

	public TimeWindow getAccWindow(long timestamp) {
		long start = ((timestamp + offset) / windowSize) * windowSize - offset;
		return new TimeWindow(start, start + windowSize);
	}

	@Override
	public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
		if (timestamp > Long.MIN_VALUE) {
			// 小窗口
			long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
			TimeWindow childWindow = new TimeWindow(start, start + slide);
			return Collections.singletonList(new TimeWindow(start, start + slide));
		} else {
			throw new RuntimeException("Record has Long.MIN_VALUE timestamp (= no timestamp marker). " +
				"Is the time characteristic set to 'ProcessingTime', or did you forget to call " +
				"'DataStream.assignTimestampsAndWatermarks(...)'?");
		}
	}

	/**
	 * 获取空的子窗口，注册事件使用.
	 * @param timestamp 时间.
	 * @return
	 */
	public TimeWindow getNextWindow(long timestamp) {
		if (timestamp > Long.MIN_VALUE) {
			// 小窗口
			long start = TimeWindow.getWindowStartWithOffset(timestamp, offset, slide);
			// 跳过当前获得子窗
			start = start + slide;
			return new TimeWindow(start, start + slide);
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

	@Override
	public String toString() {
		return "EventTimeAccumulateWindows(" + windowSize + ", " + slide + ", " + offset + ")";
	}

	public static EventTimeAccumulateWindows of(Time windowSize, Time slide, Time offset) {
		return new EventTimeAccumulateWindows(windowSize.toMilliseconds(), slide.toMilliseconds(), offset.toMilliseconds());
	}

	public static EventTimeAccumulateWindows of(Time windowSize, Time slide) {
		return new EventTimeAccumulateWindows(windowSize.toMilliseconds(), slide.toMilliseconds(), 0);
	}

	@Override
	public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
		return new TimeWindow.Serializer();
	}

	@Override
	public boolean isEventTime() {
		return true;
	}

	/**
	 * Merge overlapping {@link TimeWindow}s.
	 */
	@Override
	public void mergeWindows(Collection<TimeWindow> windows, MergeCallback<TimeWindow> c) {
		// sort the windows by the start time and then merge overlapping windows
		List<TimeWindow> sortedWindows = new ArrayList<>(windows);

		Collections.sort(sortedWindows, new Comparator<TimeWindow>() {
			@Override
			public int compare(TimeWindow o1, TimeWindow o2) {
				int cmp = Long.compare(o1.getStart(), o2.getStart());
				if (0 == cmp) {
					// 结束时间大的在前面
					cmp = -Long.compare(o1.getStart(), o2.getStart());
				}
				return cmp;
			}
		});
		if (sortedWindows.size() > 1) {
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-DD HH:mm:ss");
			for (int i = 0; i < sortedWindows.size(); i++) {
				TimeWindow tw = sortedWindows.get(i);
				System.out.println("TimeWindow:" + i + "," + sdf.format(new Date(tw.getStart())) + "," + sdf.format(new Date(tw.getEnd())));

			}
		}
		List<Tuple2<TimeWindow, Set<TimeWindow>>> merged = new ArrayList<>();
		Tuple2<TimeWindow, Set<TimeWindow>> currentMerge = null;

		for (TimeWindow candidate : sortedWindows) {
			if (currentMerge == null) {
				currentMerge = new Tuple2<>();
				currentMerge.f0 = candidate;
				currentMerge.f1 = new HashSet<>();
				currentMerge.f1.add(candidate);
			} else if (currentMerge.f0.getStart() <= candidate.getStart() && currentMerge.f0.getEnd() >= candidate.getEnd()) {
				currentMerge.f0 = currentMerge.f0.cover(candidate);
				currentMerge.f1.add(candidate);
			} else {
				merged.add(currentMerge);
				currentMerge = new Tuple2<>();
				currentMerge.f0 = candidate;
				currentMerge.f1 = new HashSet<>();
				currentMerge.f1.add(candidate);
			}
		}

		if (currentMerge != null) {
			merged.add(currentMerge);
		}

		for (Tuple2<TimeWindow, Set<TimeWindow>> m : merged) {
			if (m.f1.size() > 1) {
				c.merge(m.f1, m.f0);
			}
		}
	}

}
