package com.xxl.job.admin.core.thread;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.trigger.TriggerTypeEnum;
import com.xxl.job.admin.core.trigger.XxlJobTrigger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * job trigger thread pool helper
 *
 * @author xuxueli 2018-07-03 21:08:07
 */
public class JobTriggerPoolHelper {
	private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);


	// ---------------------- trigger pool ----------------------
	private static JobTriggerPoolHelper helper = new JobTriggerPoolHelper();
	// fast/slow thread pool
	private ThreadPoolExecutor fastTriggerPool = null;
	private ThreadPoolExecutor slowTriggerPool = null;
	// job timeout count
	private volatile long minTim = System.currentTimeMillis() / 60000;     // ms > min
	private volatile ConcurrentMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();

	public static void toStart() {
		helper.start();
	}

	public static void toStop() {
		helper.stop();
	}


	// ---------------------- helper ----------------------

	/**
	 * @param jobId
	 * @param triggerType
	 * @param failRetryCount        >=0: use this param
	 *                              <0: use param from job info config
	 * @param executorShardingParam
	 * @param executorParam         null: use job param
	 *                              not null: cover job param
	 */
	public static void trigger(int jobId, TriggerTypeEnum triggerType, int failRetryCount, String executorShardingParam, String executorParam, String addressList) {
		helper.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
	}

	/**
	 * 这里分别初始化了2个线程池，一个快一个慢，优先选择快，当一分钟以内任务超过10次执行时间超过500ms，则加入慢线程池执行。
	 */
	public void start() {
		//最大200线程，最多处理1000任务
		fastTriggerPool = new ThreadPoolExecutor(
				10,
				XxlJobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
				60L,
				TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(1000),
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode());
					}
				});

		//最大100线程，最多处理2000任务
		//一分钟内超时10次，则采用慢触发器执行
		slowTriggerPool = new ThreadPoolExecutor(
				10,
				XxlJobAdminConfig.getAdminConfig().getTriggerPoolSlowMax(),
				60L,
				TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(2000),
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						return new Thread(r, "xxl-job, admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode());
					}
				});
	}

	public void stop() {
		//triggerPool.shutdown();
		fastTriggerPool.shutdownNow();
		slowTriggerPool.shutdownNow();
		logger.info(">>>>>>>>> xxl-job trigger thread pool shutdown success.");
	}

	/**
	 * add trigger
	 * 执行任务时，首先判断这个任务是否是个慢任务，如果是个慢任务且慢执行的次数超过了10次将会使用slowTriggerPool慢线程池，
	 * 它的统计周期为60秒，这里是个优化点，当有大量的任务被执行时，为了防止任务被阻塞，尽可能的会先让执行快的任务优先执行。
	 */
	public void addTrigger(final int jobId, // 任务id
						   final TriggerTypeEnum triggerType, // 执行来源
						   final int failRetryCount,  // 失败重试次数
						   final String executorShardingParam,   // 分片广播参数
						   final String executorParam,   // 执行入参
						   final String addressList) {  // 可用执行器的地址，用逗号分割

		// choose thread pool
		// 默认使用fastTriggerPool
		ThreadPoolExecutor triggerPool_ = fastTriggerPool;
		AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
		// 如果发现任务一分钟内有大于10次的慢执行，换slowTriggerPool线程池
		if (jobTimeoutCount != null && jobTimeoutCount.get() > 10) {      // job-timeout 10 times in 1 min
			triggerPool_ = slowTriggerPool;
		}

		// trigger
		// 线程池执行
		triggerPool_.execute(new Runnable() {
			@Override
			public void run() {
				long start = System.currentTimeMillis();

				try {
					// do trigger
					// 触发
					XxlJobTrigger.trigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				} finally {

					// check timeout-count-map
					// 到达下一个周期则清理上一个周期数据
					long minTim_now = System.currentTimeMillis() / 60000;
					if (minTim != minTim_now) {
						minTim = minTim_now;
						jobTimeoutCountMap.clear();
					}

					// incr timeout-count-map
					// 记录慢任务执行次数
					long cost = System.currentTimeMillis() - start;
					if (cost > 500) {       // ob-timeout threshold 500ms
						AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
						if (timeoutCount != null) {
							timeoutCount.incrementAndGet();
						}
					}

				}

			}
		});
	}

}
