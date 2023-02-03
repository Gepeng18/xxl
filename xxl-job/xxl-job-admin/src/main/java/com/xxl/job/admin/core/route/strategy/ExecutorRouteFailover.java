package com.xxl.job.admin.core.route.strategy;

import com.xxl.job.admin.core.scheduler.XxlJobScheduler;
import com.xxl.job.admin.core.route.ExecutorRouter;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;

import java.util.List;

/**
 * Created by xuxueli on 17/3/10.
 * 当调度的机器出现无法调度的情况时，则切换为另一台机器
 * <p>
 * 实现原理就是通过调用机器的beat接口查看机器的返回状态来判定是否存活，如果不存活则循环下一个继续该步骤，直到找到可用机器或者无可用机器为止
 */
public class ExecutorRouteFailover extends ExecutorRouter {

	public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {

		StringBuffer beatResultSB = new StringBuffer();
		for (String address : addressList) {
			// beat
			ReturnT<String> beatResult = null;
			try {
				ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(address);
				// 通过调度接口的形式检查机器是否存活
				beatResult = executorBiz.beat();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				beatResult = new ReturnT<String>(ReturnT.FAIL_CODE, "" + e);
			}
			// 记录检查日志
			beatResultSB.append((beatResultSB.length() > 0) ? "<br><br>" : "")
					.append(I18nUtil.getString("jobconf_beat") + "：")
					.append("<br>address：").append(address)
					.append("<br>code：").append(beatResult.getCode())
					.append("<br>msg：").append(beatResult.getMsg());

			// beat success
			if (beatResult.getCode() == ReturnT.SUCCESS_CODE) {
				// 机器正常时则返回该机器地址
				beatResult.setMsg(beatResultSB.toString());
				beatResult.setContent(address);
				return beatResult;
			}
		}
		// 没有可用的存活机器
		return new ReturnT<String>(ReturnT.FAIL_CODE, beatResultSB.toString());
	}

}
