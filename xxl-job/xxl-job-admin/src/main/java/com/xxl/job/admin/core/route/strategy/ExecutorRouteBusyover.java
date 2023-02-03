package com.xxl.job.admin.core.route.strategy;

import com.xxl.job.admin.core.scheduler.XxlJobScheduler;
import com.xxl.job.admin.core.route.ExecutorRouter;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.IdleBeatParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;

import java.util.List;

/**
 * Created by xuxueli on 17/3/10.
 * 当机器的excutor处于忙碌的状态时，则转移至不忙碌的机器
 * <p>
 * 实现原理就是通过调用机器的idleBeat接口查看机器的返回状态来判定是否忙碌，如果处于忙碌或不可用状态则循环下一个继续该步骤，直到找到空闲且可用的机器或者没有可用机器为止
 */
public class ExecutorRouteBusyover extends ExecutorRouter {

	public ReturnT<String> route(TriggerParam triggerParam, List<String> addressList) {
		StringBuffer idleBeatResultSB = new StringBuffer();
		for (String address : addressList) {
			// beat
			ReturnT<String> idleBeatResult = null;
			try {
				ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(address);
				// 调度接口查看是否忙碌
				idleBeatResult = executorBiz.idleBeat(new IdleBeatParam(triggerParam.getJobId()));
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				idleBeatResult = new ReturnT<String>(ReturnT.FAIL_CODE, "" + e);
			}
			idleBeatResultSB.append((idleBeatResultSB.length() > 0) ? "<br><br>" : "")
					.append(I18nUtil.getString("jobconf_idleBeat") + "：")
					.append("<br>address：").append(address)
					.append("<br>code：").append(idleBeatResult.getCode())
					.append("<br>msg：").append(idleBeatResult.getMsg());

			if (idleBeatResult.getCode() == ReturnT.SUCCESS_CODE) {
				idleBeatResult.setMsg(idleBeatResultSB.toString());
				idleBeatResult.setContent(address);
				return idleBeatResult;
			}
		}
		// 没有可用的存活且空闲的机器
		return new ReturnT<String>(ReturnT.FAIL_CODE, idleBeatResultSB.toString());
	}

}
