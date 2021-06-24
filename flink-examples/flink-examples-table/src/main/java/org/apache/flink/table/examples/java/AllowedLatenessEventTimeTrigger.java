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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.table.runtime.types.CRow;

/**
 * 允许延迟数据进入窗口计算的trigger
 */
public class AllowedLatenessEventTimeTrigger extends Trigger<CRow, Window> {

	private static final long serialVersionUID = 1L;

	// 基于processingTime的统计频率
	private final long processingTimeInterval;

	// 允许进入窗口计算的延迟时长
	private final long allowedLatenessEventTime;

	private AllowedLatenessEventTimeTrigger(long processingTimeInterval, long allowedLatenessEventTime) {
		this.processingTimeInterval = processingTimeInterval;
		this.allowedLatenessEventTime = allowedLatenessEventTime;
	}

	/**
	 * When merging we take the lowest of all fire timestamps as the new fire timestamp.
	 */
	private final ReducingStateDescriptor<Long> stateDesc =
		new ReducingStateDescriptor<>("fire-time-allowed-lateness", new Min(), LongSerializer.INSTANCE);

	@Override
	public TriggerResult onElement(CRow element, long timestamp, Window window, TriggerContext ctx) throws Exception {
		if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
			// 如果watermark已经越过了窗口，则注册一个指定时间基于ProcessingTime定时器，到时间后fire窗口
			ReducingState<Long> fireTimestamp = ctx.getPartitionedState(stateDesc);
			if (fireTimestamp.get() == null) {
				timestamp = ctx.getCurrentProcessingTime();
				long nextFireTimestamp = timestamp + this.processingTimeInterval;
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
	public TriggerResult onProcessingTime(long timestamp, Window window, TriggerContext ctx) throws Exception {
		return TriggerResult.FIRE_AND_PURGE;
	}

	@Override
	public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
		// fire 同时清理窗口state，使得延迟时间是增量的计算。
		if (time == window.maxTimestamp()) {
			return TriggerResult.FIRE_AND_PURGE;
		} else if (time == window.maxTimestamp() + allowedLatenessEventTime) {
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
	public void onMerge(Window window, OnMergeContext ctx) throws Exception {
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
	 * 根据参数创建允许延迟数据进入窗口计算的trigger
	 *
	 * @param processingTimeInterval 延迟数据推出窗口计算结果的间隔，以秒为单位
	 * @param allowedLatenessEventTime 允许延迟时间内的数据进入窗口计算，以小时为单位
	 * @return AllowedLatenessEventTimeTrigger
	 */
	public static AllowedLatenessEventTimeTrigger of(int processingTimeInterval, int allowedLatenessEventTime) {
		return new AllowedLatenessEventTimeTrigger(
			Time.seconds(processingTimeInterval).toMilliseconds(),
			Time.hours(allowedLatenessEventTime).toMilliseconds()
		);
	}

	private static class Min implements ReduceFunction<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long reduce(Long value1, Long value2) throws Exception {
			return Math.min(value1, value2);
		}
	}
}
