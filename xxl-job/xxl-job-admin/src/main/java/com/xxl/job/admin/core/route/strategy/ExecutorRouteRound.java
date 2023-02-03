package com.xxl.job.admin.core.route.strategy;

import com.xxl.job.admin.core.route.ExecutorRouter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;

import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by xuxueli on 17/3/10.
 * 轮询并非是从第一个开始，而是随机选择开始的位置，每次通过自增后取模来定位到下一个地址，为了防止integer无限增大，每24小时会清除一次位置信息，重新随机定位。
 */
public class ExecutorRouteRound extends ExecutorRouter {

	private static ConcurrentMap<Integer, AtomicInteger> routeCountEachJob = new ConcurrentHashMap<>();
	private static long CACHE_VALID_TIME = 0;

	private static int count(int jobId) {
		// cache clear
		if (System.currentTimeMillis() > CACHE_VALID_TIME) {
			// 缓存超时清除所有任务的位置
			routeCountEachJob.clear();
			// 缓存24小时
			CACHE_VALID_TIME = System.currentTimeMillis() + 1000 * 60 * 60 * 24;
		}

		// 获得任务的随机数
		AtomicInteger count = routeCountEachJob.get(jobId);
		if (count == null || count.get() > 1000000) {
			// 初始化时主动Random一次，缓解首次压力
			count = new AtomicInteger(new Random().nextInt(100));
		} else {
			// 获得下一个任务，count++
			count.addAndGet(1);
		}
		routeCountEachJob.put(jobId, count);
		return count.get();
	}

	@Override
	public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
		// 通过取模来定位到下一个执行的地址
		String address = addressList.get(count(triggerParam.getJobId()) % addressList.size());
		return new ReturnT<String>(address);
	}

}
