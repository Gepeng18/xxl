package com.xxl.rpc.core.remoting.invoker.route.impl;

import com.xxl.rpc.core.remoting.invoker.route.XxlRpcLoadBalance;

import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * round
 *
 * @author xuxueli 2018-12-04
 */
public class XxlRpcLoadBalanceRoundStrategy extends XxlRpcLoadBalance {
    // 用于记录某个服务 次数的 通过这个次数 % 服务提供者数量  得到 选择哪个位置的机器
    private ConcurrentMap<String, AtomicInteger> routeCountEachJob = new ConcurrentHashMap<String, AtomicInteger>();
    private long CACHE_VALID_TIME = 0;
    private int count(String serviceKey) {
        // cache clear
        // cache clear  过段时间就要清一下 map
        if (System.currentTimeMillis() > CACHE_VALID_TIME) {
            routeCountEachJob.clear();
            CACHE_VALID_TIME = System.currentTimeMillis() + 24*60*60*1000;
        }

        // count++
        AtomicInteger count = routeCountEachJob.get(serviceKey);
        if (count == null || count.get() > 1000000) {
            // 超过 1000000 就要 初始化，初始化的时候使用随机数，防止第一次老往第一台机器上扔
            // 初始化时主动Random一次，缓解首次压力
            count = new AtomicInteger(new Random().nextInt(100));
        } else {
            // count++
            count.addAndGet(1);
        }

        routeCountEachJob.put(serviceKey, count);
        return count.get();
    }

    @Override
    public String route(String serviceKey, TreeSet<String> addressSet) {
        // arr
        String[] addressArr = addressSet.toArray(new String[addressSet.size()]);

        // round
        String finalAddress = addressArr[count(serviceKey)%addressArr.length];
        return finalAddress;
    }

}
