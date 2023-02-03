package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.cron.CronExpression;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.scheduler.MisfireStrategyEnum;
import com.xxl.job.admin.core.scheduler.ScheduleTypeEnum;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * @author xuxueli 2019-05-21
 */
public class JobScheduleHelper {
	// 任务间隔大小
	public static final long PRE_READ_MS = 5000;    // pre read
	private static Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);
	private static JobScheduleHelper instance = new JobScheduleHelper();
	// 时间轮，key 0-59，value 任务ID列表，两个线程同时处理这个对象
	private volatile static Map<Integer, List<Integer>> ringData = new ConcurrentHashMap<>();
	private Thread scheduleThread;
	private Thread ringThread;
	private volatile boolean scheduleThreadToStop = false;
	private volatile boolean ringThreadToStop = false;

	public static JobScheduleHelper getInstance() {
		return instance;
	}

	// ---------------------- tools ----------------------
	public static Date generateNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
		//匹配调度类型
		ScheduleTypeEnum scheduleTypeEnum = ScheduleTypeEnum.match(jobInfo.getScheduleType(), null);
		if (ScheduleTypeEnum.CRON == scheduleTypeEnum) {
			//通过CRON，触发任务调度；
			//通过cron表达式,获取下一次执行时间
			Date nextValidTime = new CronExpression(jobInfo.getScheduleConf()).getNextValidTimeAfter(fromTime);
			return nextValidTime;
		} else if (ScheduleTypeEnum.FIX_RATE == scheduleTypeEnum /*|| ScheduleTypeEnum.FIX_DELAY == scheduleTypeEnum*/) {
			//以固定速度，触发任务调度；按照固定的间隔时间，周期性触发；
			return new Date(fromTime.getTime() + Integer.valueOf(jobInfo.getScheduleConf()) * 1000);
		}
		//没有匹配到调度类型,则null
		return null;
	}

	public void start() {

		// schedule thread
		// 初始化一个调度线程，5秒执行一次，查询 当前时间 + 5000 毫秒，就是接下来 5 秒 《之前》 要执行的所有任务，
		// 如果触发时间小于now，即本该以及执行的任务，则直接远程调度，如果是将来要执行的任务，则放入ringData中
		// （放入的index为下一次执行的时间 % 60），并且更新下一次执行时间进jobInfo表中
		scheduleThread = new Thread(new Runnable() {
			@Override
			public void run() {

				try {
					// 5秒执行一次
					TimeUnit.MILLISECONDS.sleep(5000 - System.currentTimeMillis() % 1000);
				} catch (InterruptedException e) {
					if (!scheduleThreadToStop) {
						logger.error(e.getMessage(), e);
					}
				}
				logger.info(">>>>>>>>> init xxl-job admin scheduler success.");

				// pre-read count: treadpool-size * trigger-qps (each trigger cost 50ms, qps = 1000/50 = 20)
				// 每次读取到任务个数
				int preReadCount = (XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax() + XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax()) * 20;

				while (!scheduleThreadToStop) {

					// Scan Job
					long start = System.currentTimeMillis();

					Connection conn = null;
					Boolean connAutoCommit = null;
					PreparedStatement preparedStatement = null;

					boolean preReadSuc = true;
					try {

						conn = XxlJobAdminConfig.getAdminConfig().getDataSource().getConnection();
						connAutoCommit = conn.getAutoCommit();
						conn.setAutoCommit(false);

						// 分布式下获取锁
						preparedStatement = conn.prepareStatement("select * from xxl_job_lock where lock_name = 'schedule_lock' for update");
						preparedStatement.execute();

						// tx start

						long nowTime = System.currentTimeMillis();
						// 1、查询 当前时间 + 5000 毫秒，就是接下来 5 秒 《之前》 要执行的所有任务
						List<XxlJobInfo> scheduleList = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleJobQuery(nowTime + PRE_READ_MS, preReadCount);
						if (scheduleList != null && scheduleList.size() > 0) {
							// 2、push time-ring
							for (XxlJobInfo jobInfo : scheduleList) {

								// time-ring jump
								// |----任务在这里----|---------------------|-------------------------|
								// ---------------now-5s------------------now---------------------now+5s
								// if jobInfo.getTriggerNextTime() < nowTime - 5s
								if (nowTime > jobInfo.getTriggerNextTime() + PRE_READ_MS) {
									// 2.1、trigger-expire > 5s：pass && make next-trigger-time
									logger.warn(">>>>>>>>>>> xxl-job, schedule misfire, jobId = " + jobInfo.getId());

									// 1、misfire match
									// 如果每次都有执行则立即触发执行
									// 忽略:调度过期后,忽略过期的任务,从当前时间开始重新计算下次触发时间;
									// 立即执行一次,调度过期后,立即地行一次,并1当前时间开始重新计算下次触发时间:
									MisfireStrategyEnum misfireStrategyEnum = MisfireStrategyEnum.match(jobInfo.getMisfireStrategy(), MisfireStrategyEnum.DO_NOTHING);
									if (MisfireStrategyEnum.FIRE_ONCE_NOW == misfireStrategyEnum) {
										// 若过期策略为FIRE ONCE NOW,则立即执行一次
										// FIRE_ONCE_NOW 》 trigger 执行出发器
										JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.MISFIRE, -1, null, null, null);
										logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId());
									}

									// 2、fresh next
									// 刷新接下来要执行时间
									refreshNextValidTime(jobInfo, new Date());

								} else if (nowTime > jobInfo.getTriggerNextTime()) {
									// |---------------|------任务在这里--------|-------------------------|
									// ---------------now-5s------------------now---------------------now+5s
									// 2.2、trigger-expire < 5s：direct-trigger && make next-trigger-time

									// 1、trigger
									// 如果当前时间大于接下来要执行到时间则立即触发执行
									JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1, null, null, null);
									logger.debug(">>>>>>>>>>> xxl-job, schedule push trigger : jobId = " + jobInfo.getId());

									// 2、fresh next
									// 刷新下次执行时间
									refreshNextValidTime(jobInfo, new Date());

									// next-trigger-time in 5s, pre-read again
									// |---------------|----------------------|---------任务在这里----------|
									// ---------------now-5s------------------now---------------------now+5s
									if (jobInfo.getTriggerStatus() == 1 && nowTime + PRE_READ_MS > jobInfo.getTriggerNextTime()) {

										// 1、make ring second
										// 如果接下来 5 秒内还执行则直接放到时间轮中
										int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);

										// 2、push time ring
										pushTimeRing(ringSecond, jobInfo.getId());

										// 3、fresh next
										// 刷新下次执行时间
										refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));

									}

								} else {
									// |---------------|----------------------|---------任务在这里----------|
									// ---------------now-5s------------------now---------------------now+5s
									// 2.3、trigger-pre-read：time-ring trigger && make next-trigger-time

									// 1、make ring second
									// 任务还没有到执行时间则直接放到时间轮中
									int ringSecond = (int) ((jobInfo.getTriggerNextTime() / 1000) % 60);

									// 2、push time ring
									pushTimeRing(ringSecond, jobInfo.getId());

									// 3、fresh next
									// 刷新下次执行时间
									refreshNextValidTime(jobInfo, new Date(jobInfo.getTriggerNextTime()));

								}

							}

							// 3、update trigger info
							for (XxlJobInfo jobInfo : scheduleList) {
								// 更新任务信息
								XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().scheduleUpdate(jobInfo);
							}

						} else {
							preReadSuc = false;
						}

						// tx stop


					} catch (Exception e) {
						if (!scheduleThreadToStop) {
							logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread error:{}", e);
						}
					} finally {

						// commit
						if (conn != null) {
							try {
								conn.commit();
							} catch (SQLException e) {
								if (!scheduleThreadToStop) {
									logger.error(e.getMessage(), e);
								}
							}
							try {
								conn.setAutoCommit(connAutoCommit);
							} catch (SQLException e) {
								if (!scheduleThreadToStop) {
									logger.error(e.getMessage(), e);
								}
							}
							try {
								conn.close();
							} catch (SQLException e) {
								if (!scheduleThreadToStop) {
									logger.error(e.getMessage(), e);
								}
							}
						}

						// close PreparedStatement
						if (null != preparedStatement) {
							try {
								preparedStatement.close();
							} catch (SQLException e) {
								if (!scheduleThreadToStop) {
									logger.error(e.getMessage(), e);
								}
							}
						}
					}
					long cost = System.currentTimeMillis() - start;


					// Wait seconds, align second
					if (cost < 1000) {  // scan-overtime, not wait
						try {
							// pre-read period: success > scan each second; fail > skip this period;
							TimeUnit.MILLISECONDS.sleep((preReadSuc ? 1000 : PRE_READ_MS) - System.currentTimeMillis() % 1000);
						} catch (InterruptedException e) {
							if (!scheduleThreadToStop) {
								logger.error(e.getMessage(), e);
							}
						}
					}

				}

				logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#scheduleThread stop");
			}
		});
		scheduleThread.setDaemon(true);
		scheduleThread.setName("xxl-job, admin JobScheduleHelper#scheduleThread");
		scheduleThread.start();


		// ring thread
		// 初始化一个时间轮线程，1秒执行一次，主要工作为：从ringData中获取最近0秒和1秒将要执行的任务，进行远程触发。
		ringThread = new Thread(new Runnable() {
			@Override
			public void run() {

				while (!ringThreadToStop) {

					// align second
					// 时间对齐，对齐至秒
					try {
						TimeUnit.MILLISECONDS.sleep(1000 - System.currentTimeMillis() % 1000);
					} catch (InterruptedException e) {
						if (!ringThreadToStop) {
							logger.error(e.getMessage(), e);
						}
					}

					try {
						// second data
						// 时间轮数据处理
						List<Integer> ringItemData = new ArrayList<>();
						int nowSecond = Calendar.getInstance().get(Calendar.SECOND);   // 避免处理耗时太长，跨过刻度，向前校验一个刻度；
						// 获取前1秒和2秒要执行到任务
						for (int i = 0; i < 2; i++) {
							List<Integer> tmpData = ringData.remove((nowSecond + 60 - i) % 60);
							if (tmpData != null) {
								ringItemData.addAll(tmpData);
							}
						}

						// ring trigger
						logger.debug(">>>>>>>>>>> xxl-job, time-ring beat : " + nowSecond + " = " + Arrays.asList(ringItemData));
						if (ringItemData.size() > 0) {
							// do trigger
							for (int jobId : ringItemData) {
								// do trigger
								// 执行任务
								JobTriggerPoolHelper.trigger(jobId, TriggerTypeEnum.CRON, -1, null, null, null);
							}
							// clear
							ringItemData.clear();
						}
					} catch (Exception e) {
						if (!ringThreadToStop) {
							logger.error(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread error:{}", e);
						}
					}
				}
				logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper#ringThread stop");
			}
		});
		ringThread.setDaemon(true);
		ringThread.setName("xxl-job, admin JobScheduleHelper#ringThread");
		ringThread.start();
	}

	private void refreshNextValidTime(XxlJobInfo jobInfo, Date fromTime) throws Exception {
		Date nextValidTime = generateNextValidTime(jobInfo, fromTime);
		if (nextValidTime != null) {
			jobInfo.setTriggerLastTime(jobInfo.getTriggerNextTime());
			jobInfo.setTriggerNextTime(nextValidTime.getTime());
		} else {
			jobInfo.setTriggerStatus(0);
			jobInfo.setTriggerLastTime(0);
			jobInfo.setTriggerNextTime(0);
			logger.warn(">>>>>>>>>>> xxl-job, refreshNextValidTime fail for job: jobId={}, scheduleType={}, scheduleConf={}",
					jobInfo.getId(), jobInfo.getScheduleType(), jobInfo.getScheduleConf());
		}
	}

	// ringSecond 下一次执行的时间 % 60
	private void pushTimeRing(int ringSecond, int jobId) {
		// push async ring
		List<Integer> ringItemData = ringData.get(ringSecond);
		if (ringItemData == null) {
			ringItemData = new ArrayList<Integer>();
			ringData.put(ringSecond, ringItemData);
		}
		ringItemData.add(jobId);

		logger.debug(">>>>>>>>>>> xxl-job, schedule push time-ring : " + ringSecond + " = " + Arrays.asList(ringItemData));
	}

	public void toStop() {

		// 1、stop schedule
		scheduleThreadToStop = true;
		try {
			TimeUnit.SECONDS.sleep(1);  // wait
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
		if (scheduleThread.getState() != Thread.State.TERMINATED) {
			// interrupt and wait
			scheduleThread.interrupt();
			try {
				scheduleThread.join();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}

		// if has ring data
		boolean hasRingData = false;
		if (!ringData.isEmpty()) {
			for (int second : ringData.keySet()) {
				List<Integer> tmpData = ringData.get(second);
				if (tmpData != null && tmpData.size() > 0) {
					hasRingData = true;
					break;
				}
			}
		}
		if (hasRingData) {
			try {
				TimeUnit.SECONDS.sleep(8);
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}

		// stop ring (wait job-in-memory stop)
		ringThreadToStop = true;
		try {
			TimeUnit.SECONDS.sleep(1);
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
		if (ringThread.getState() != Thread.State.TERMINATED) {
			// interrupt and wait
			ringThread.interrupt();
			try {
				ringThread.join();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}

		logger.info(">>>>>>>>>>> xxl-job, JobScheduleHelper stop");
	}

}
