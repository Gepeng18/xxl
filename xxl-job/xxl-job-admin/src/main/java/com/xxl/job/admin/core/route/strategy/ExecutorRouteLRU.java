package com.xxl.job.admin.core.route.strategy;

import com.xxl.job.admin.core.route.ExecutorRouter;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 单个JOB对应的每个执行器，最久为使用的优先被选举
 * a、LFU(Least Frequently Used)：最不经常使用，频率/次数
 * b(*)、LRU(Least Recently Used)：最近最久未使用，时间
 * <p>
 * 维护了一个以任务id为单位的map，kv都是地址，实现原理是利用了LinkedHashMap存储排序的特性
 * accessOrder：true=访问顺序排序（get/put时排序）；false=插入顺序排期；
 */
public class ExecutorRouteLRU extends ExecutorRouter {

	// key=jobId,value-key=address,value-value=address
	private static ConcurrentMap<Integer, LinkedHashMap<String, String>> jobLRUMap = new ConcurrentHashMap<Integer, LinkedHashMap<String, String>>();
	private static long CACHE_VALID_TIME = 0;

	public String route(int jobId, List<String> addressList) {

		// 每24小时清除一次缓存
		if (System.currentTimeMillis() > CACHE_VALID_TIME) {
			jobLRUMap.clear();
			CACHE_VALID_TIME = System.currentTimeMillis() + 1000 * 60 * 60 * 24;
		}

		// init lru
		LinkedHashMap<String, String> lruItem = jobLRUMap.get(jobId);
		if (lruItem == null) {
			/**
			 * LinkedHashMap
			 *      a、accessOrder：true=访问顺序排序（get/put时排序）；false=插入顺序排期；
			 *      b、removeEldestEntry：新增元素时将会调用，返回true时会删除最老元素；可封装LinkedHashMap并重写该方法，比如定义最大容量，超出是返回true即可实现固定长度的LRU算法；
			 */
			lruItem = new LinkedHashMap<String, String>(16, 0.75f, true);
			jobLRUMap.putIfAbsent(jobId, lruItem);
		}

		// 初始化地址kv都是地址
		for (String address : addressList) {
			if (!lruItem.containsKey(address)) {
				lruItem.put(address, address);
			}
		}
		// 移除无效的地址
		List<String> delKeys = new ArrayList<>();
		for (String existKey : lruItem.keySet()) {
			if (!addressList.contains(existKey)) {
				delKeys.add(existKey);
			}
		}
		if (delKeys.size() > 0) {
			for (String delKey : delKeys) {
				lruItem.remove(delKey);
			}
		}

		// 直接拿到第一个即可
		String eldestKey = lruItem.entrySet().iterator().next().getKey();
		String eldestValue = lruItem.get(eldestKey);
		return eldestValue;
	}

	@Override
	public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
		String address = route(triggerParam.getJobId(), addressList);
		return new ReturnT<String>(address);
	}

}
