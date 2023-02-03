package com.xxl.job.admin.core.trigger;

import com.xxl.job.admin.core.conf.XxlJobAdminConfig;
import com.xxl.job.admin.core.model.XxlJobGroup;
import com.xxl.job.admin.core.model.XxlJobInfo;
import com.xxl.job.admin.core.model.XxlJobLog;
import com.xxl.job.admin.core.route.ExecutorRouteStrategyEnum;
import com.xxl.job.admin.core.scheduler.XxlJobScheduler;
import com.xxl.job.admin.core.util.I18nUtil;
import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.biz.model.TriggerParam;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.job.core.util.IpUtil;
import com.xxl.job.core.util.ThrowableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * xxl-job trigger
 * Created by xuxueli on 17/7/13.
 */
public class XxlJobTrigger {
	private static Logger logger = LoggerFactory.getLogger(XxlJobTrigger.class);

	/**
	 * trigger job
	 *
	 * @param jobId
	 * @param triggerType
	 * @param failRetryCount        >=0: use this param
	 *                              <0: use param from job info config
	 * @param executorShardingParam
	 * @param executorParam         null: use job param
	 *                              not null: cover job param
	 * @param addressList           null: use executor addressList
	 *                              not null: cover
	 */
	public static void trigger(int jobId,   // 任务id
							   TriggerTypeEnum triggerType,  // 执行来源
							   int failRetryCount,   // 失败重试次数
							   String executorShardingParam,    // 分片广播参数
							   String executorParam,   // 执行入参
							   String addressList) {  // 可用执行器的地址，用逗号分割

		// load data
		// 从JobInfo表中获取任务信息
		XxlJobInfo jobInfo = XxlJobAdminConfig.getAdminConfig().getXxlJobInfoDao().loadById(jobId);
		if (jobInfo == null) {
			logger.warn(">>>>>>>>>>>> trigger fail, jobId invalid，jobId={}", jobId);
			return;
		}
		// 设置任务入参
		if (executorParam != null) {
			jobInfo.setExecutorParam(executorParam);
		}
		int finalFailRetryCount = failRetryCount >= 0 ? failRetryCount : jobInfo.getExecutorFailRetryCount();
		// 根据任务的分组信息找到分组，分组中存在服务器的IP和端口地址等
		// 获得执行器
		XxlJobGroup group = XxlJobAdminConfig.getAdminConfig().getXxlJobGroupDao().load(jobInfo.getJobGroup());

		// cover addressList
		// 录入执行器地址
		if (addressList != null && addressList.trim().length() > 0) {
			group.setAddressType(1);
			group.setAddressList(addressList.trim());
		}

		// sharding param
		// 分片广播的逻辑
		int[] shardingParam = null;
		if (executorShardingParam != null) {
			String[] shardingArr = executorShardingParam.split("/");
			if (shardingArr.length == 2 && isNumeric(shardingArr[0]) && isNumeric(shardingArr[1])) {
				shardingParam = new int[2];
				shardingParam[0] = Integer.valueOf(shardingArr[0]);
				shardingParam[1] = Integer.valueOf(shardingArr[1]);
			}
		}

		// 如果是分片广播，同时注册地址不为空则特殊处理
		if (ExecutorRouteStrategyEnum.SHARDING_BROADCAST == ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null)
				&& group.getRegistryList() != null && !group.getRegistryList().isEmpty()
				&& shardingParam == null) {
			for (int i = 0; i < group.getRegistryList().size(); i++) {
				// 分片广播会通知每一个执行器
				processTrigger(group, jobInfo, finalFailRetryCount, triggerType, i, group.getRegistryList().size());
			}
		} else {
			if (shardingParam == null) {
				shardingParam = new int[]{0, 1};
			}
			// do 其他执行策略
			processTrigger(group, jobInfo, finalFailRetryCount, triggerType, shardingParam[0], shardingParam[1]);
		}

	}

	private static boolean isNumeric(String str) {
		try {
			int result = Integer.valueOf(str);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	/**
	 * @param group               job group, registry list may be empty
	 * @param jobInfo
	 * @param finalFailRetryCount
	 * @param triggerType
	 * @param index               sharding index
	 * @param total               sharding total
	 */
	private static void processTrigger(XxlJobGroup group, XxlJobInfo jobInfo, int finalFailRetryCount, TriggerTypeEnum triggerType, int index, int total) {

		// param
		// 阻塞策略(先判断并行还是串行)
		ExecutorBlockStrategyEnum blockStrategy = ExecutorBlockStrategyEnum.match(jobInfo.getExecutorBlockStrategy(), ExecutorBlockStrategyEnum.SERIAL_EXECUTION);  // block strategy
		// 获取路由策略,默认first
		ExecutorRouteStrategyEnum executorRouteStrategyEnum = ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null);    // route strategy
		// 分片广播参数
		String shardingParam = (ExecutorRouteStrategyEnum.SHARDING_BROADCAST == executorRouteStrategyEnum) ? String.valueOf(index).concat("/").concat(String.valueOf(total)) : null;

		// 1、save log-id
		// 存储任务执行日志
		XxlJobLog jobLog = new XxlJobLog();
		jobLog.setJobGroup(jobInfo.getJobGroup());
		jobLog.setJobId(jobInfo.getId());
		jobLog.setTriggerTime(new Date());
		XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().save(jobLog);
		logger.debug(">>>>>>>>>>> xxl-job trigger start, jobId:{}", jobLog.getId());

		// 2、init trigger-param
		// 初始化请求参数
		TriggerParam triggerParam = new TriggerParam();
		triggerParam.setJobId(jobInfo.getId());
		triggerParam.setExecutorHandler(jobInfo.getExecutorHandler());
		triggerParam.setExecutorParams(jobInfo.getExecutorParam());
		triggerParam.setExecutorBlockStrategy(jobInfo.getExecutorBlockStrategy());
		triggerParam.setExecutorTimeout(jobInfo.getExecutorTimeout());
		triggerParam.setLogId(jobLog.getId());
		triggerParam.setLogDateTime(jobLog.getTriggerTime().getTime());
		triggerParam.setGlueType(jobInfo.getGlueType());
		triggerParam.setGlueSource(jobInfo.getGlueSource());
		triggerParam.setGlueUpdatetime(jobInfo.getGlueUpdatetime().getTime());
		triggerParam.setBroadcastIndex(index);
		triggerParam.setBroadcastTotal(total);

		// 3、init address
		// 决策路由执行地址
		String address = null;
		ReturnT<String> routeAddressResult = null;
		if (group.getRegistryList() != null && !group.getRegistryList().isEmpty()) {
			if (ExecutorRouteStrategyEnum.SHARDING_BROADCAST == executorRouteStrategyEnum) {
				// 分片广播逻辑
				if (index < group.getRegistryList().size()) {
					address = group.getRegistryList().get(index);
				} else {
					address = group.getRegistryList().get(0);
				}
			} else {
				// 通过我们指定的策略选择地址
				routeAddressResult = executorRouteStrategyEnum.getRouter().route(triggerParam, group.getRegistryList());
				if (routeAddressResult.getCode() == ReturnT.SUCCESS_CODE) {
					address = routeAddressResult.getContent();
				}
			}
		} else {
			routeAddressResult = new ReturnT<String>(ReturnT.FAIL_CODE, I18nUtil.getString("jobconf_trigger_address_empty"));
		}

		// do 4、trigger remote executor
		// 触发远程执行
		ReturnT<String> triggerResult = null;
		if (address != null) {
			triggerResult = runExecutor(triggerParam, address);
		} else {
			triggerResult = new ReturnT<String>(ReturnT.FAIL_CODE, null);
		}

		// 5、collection trigger info
		// 日志信息拼接
		StringBuffer triggerMsgSb = new StringBuffer();
		triggerMsgSb.append(I18nUtil.getString("jobconf_trigger_type")).append("：").append(triggerType.getTitle());
		triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_admin_adress")).append("：").append(IpUtil.getIp());
		triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_exe_regtype")).append("：")
				.append((group.getAddressType() == 0) ? I18nUtil.getString("jobgroup_field_addressType_0") : I18nUtil.getString("jobgroup_field_addressType_1"));
		triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_exe_regaddress")).append("：").append(group.getRegistryList());
		triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorRouteStrategy")).append("：").append(executorRouteStrategyEnum.getTitle());
		if (shardingParam != null) {
			triggerMsgSb.append("(" + shardingParam + ")");
		}
		triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorBlockStrategy")).append("：").append(blockStrategy.getTitle());
		triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_timeout")).append("：").append(jobInfo.getExecutorTimeout());
		triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorFailRetryCount")).append("：").append(finalFailRetryCount);

		triggerMsgSb.append("<br><br><span style=\"color:#00c0ef;\" > >>>>>>>>>>>" + I18nUtil.getString("jobconf_trigger_run") + "<<<<<<<<<<< </span><br>")
				.append((routeAddressResult != null && routeAddressResult.getMsg() != null) ? routeAddressResult.getMsg() + "<br><br>" : "").append(triggerResult.getMsg() != null ? triggerResult.getMsg() : "");

		// 6、save log trigger-info
		// 存储任务执行日志信息
		jobLog.setExecutorAddress(address);
		jobLog.setExecutorHandler(jobInfo.getExecutorHandler());
		jobLog.setExecutorParam(jobInfo.getExecutorParam());
		jobLog.setExecutorShardingParam(shardingParam);
		jobLog.setExecutorFailRetryCount(finalFailRetryCount);
		//jobLog.setTriggerTime();
		jobLog.setTriggerCode(triggerResult.getCode());
		jobLog.setTriggerMsg(triggerMsgSb.toString());
		XxlJobAdminConfig.getAdminConfig().getXxlJobLogDao().updateTriggerInfo(jobLog);

		logger.debug(">>>>>>>>>>> xxl-job trigger end, jobId:{}", jobLog.getId());
	}

	/**
	 * run executor
	 *
	 * @param triggerParam 调用的所有参数
	 * @param address      client的地址
	 * @return
	 */
	public static ReturnT<String> runExecutor(TriggerParam triggerParam, String address) {
		ReturnT<String> runResult = null;
		try {
			// 获取业务执行器地址,就执行器地址后面拼接token
			ExecutorBiz executorBiz = XxlJobScheduler.getExecutorBiz(address);
			// 这里是Server，Server调用的是 ExecutorBizClient.run()
			// client端执行的是 ExecutorBizImpl.run()
			runResult = executorBiz.run(triggerParam);
		} catch (Exception e) {
			logger.error(">>>>>>>>>>> xxl-job trigger error, please check if the executor[{}] is running.", address, e);
			runResult = new ReturnT<String>(ReturnT.FAIL_CODE, ThrowableUtil.toString(e));
		}

		StringBuffer runResultSB = new StringBuffer(I18nUtil.getString("jobconf_trigger_run") + "：");
		runResultSB.append("<br>address：").append(address);
		runResultSB.append("<br>code：").append(runResult.getCode());
		runResultSB.append("<br>msg：").append(runResult.getMsg());

		runResult.setMsg(runResultSB.toString());
		return runResult;
	}

}
