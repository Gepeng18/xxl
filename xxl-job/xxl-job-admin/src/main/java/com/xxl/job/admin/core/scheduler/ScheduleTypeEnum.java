package com.xxl.job.admin.core.scheduler;

import com.xxl.job.admin.core.util.I18nUtil;

/**
 * @author xuxueli 2020-10-29 21:11:23
 */
public enum ScheduleTypeEnum {

	// 该类型不会主动触发调度；
	NONE(I18nUtil.getString("schedule_type_none")),

	/**
	 * schedule by cron
	 * 该类型将会通过CRON，触发任务调度；
	 */
	CRON(I18nUtil.getString("schedule_type_cron")),

	/**
	 * schedule by fixed rate (in seconds)
	 * 固定速度：该类型将会以固定速度，触发任务调度；按照固定的间隔时间，周期性触发；
	 */
	FIX_RATE(I18nUtil.getString("schedule_type_fix_rate")),

	/**
	 * schedule by fix delay (in seconds)， after the last time
	 */
	/*FIX_DELAY(I18nUtil.getString("schedule_type_fix_delay"))*/;

	private String title;

	ScheduleTypeEnum(String title) {
		this.title = title;
	}

	public static ScheduleTypeEnum match(String name, ScheduleTypeEnum defaultItem) {
		for (ScheduleTypeEnum item : ScheduleTypeEnum.values()) {
			if (item.name().equals(name)) {
				return item;
			}
		}
		return defaultItem;
	}

	public String getTitle() {
		return title;
	}

}
