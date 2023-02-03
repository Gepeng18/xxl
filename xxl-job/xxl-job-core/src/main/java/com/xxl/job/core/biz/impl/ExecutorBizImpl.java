package com.xxl.job.core.biz.impl;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.model.*;
import com.xxl.job.core.enums.ExecutorBlockStrategyEnum;
import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.glue.GlueTypeEnum;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.impl.GlueJobHandler;
import com.xxl.job.core.handler.impl.ScriptJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.thread.JobThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * Created by xuxueli on 17/3/1.
 */
public class ExecutorBizImpl implements ExecutorBiz {
	private static Logger logger = LoggerFactory.getLogger(ExecutorBizImpl.class);

	@Override
	public ReturnT<String> beat() {
		return ReturnT.SUCCESS;
	}

	@Override
	public ReturnT<String> idleBeat(IdleBeatParam idleBeatParam) {

		// isRunningOrHasQueue
		boolean isRunningOrHasQueue = false;
		JobThread jobThread = XxlJobExecutor.loadJobThread(idleBeatParam.getJobId());
		if (jobThread != null && jobThread.isRunningOrHasQueue()) {
			isRunningOrHasQueue = true;
		}

		if (isRunningOrHasQueue) {
			return new ReturnT<String>(ReturnT.FAIL_CODE, "job thread is running or has trigger queue.");
		}
		return ReturnT.SUCCESS;
	}

	/**
	 * 该方法被调用的最开始的起点是 EmbedHttpServerHandler
	 * 1. 绑定作业到具体线程，启动线程
	 * 2. 任务丢入线程处理
	 */
	@Override
	public ReturnT<String> run(TriggerParam triggerParam) {
		// load old：jobHandler + jobThread
		// 获得执行控制器 （从jobThreadRepository中拿）
		JobThread jobThread = XxlJobExecutor.loadJobThread(triggerParam.getJobId());
		IJobHandler jobHandler = jobThread != null ? jobThread.getHandler() : null;
		String removeOldReason = null;

		// valid：jobHandler + jobThread
		GlueTypeEnum glueTypeEnum = GlueTypeEnum.match(triggerParam.getGlueType());
		// do bean模式触发
		if (GlueTypeEnum.BEAN == glueTypeEnum) {

			// new jobhandler （从jobHandlerRepository中拿）
			IJobHandler newJobHandler = XxlJobExecutor.loadJobHandler(triggerParam.getExecutorHandler());

			// valid old jobThread
			if (jobThread != null && jobHandler != newJobHandler) {
				// change handler, need kill old thread
				removeOldReason = "change jobhandler or glue type, and terminate the old job thread.";

				jobThread = null;
				jobHandler = null;
			}

			// valid handler
			if (jobHandler == null) {
				jobHandler = newJobHandler;
				if (jobHandler == null) {
					return new ReturnT<String>(ReturnT.FAIL_CODE, "job handler [" + triggerParam.getExecutorHandler() + "] not found.");
				}
			}

		} else if (GlueTypeEnum.GLUE_GROOVY == glueTypeEnum) {
			// 原生模式触发
			// valid old jobThread
			if (jobThread != null &&
					!(jobThread.getHandler() instanceof GlueJobHandler
							&& ((GlueJobHandler) jobThread.getHandler()).getGlueUpdatetime() == triggerParam.getGlueUpdatetime())) {
				// change handler or gluesource updated, need kill old thread
				removeOldReason = "change job source or glue type, and terminate the old job thread.";

				jobThread = null;
				jobHandler = null;
			}

			// valid handler
			if (jobHandler == null) {
				try {
					IJobHandler originJobHandler = GlueFactory.getInstance().loadNewInstance(triggerParam.getGlueSource());
					jobHandler = new GlueJobHandler(originJobHandler, triggerParam.getGlueUpdatetime());
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
					return new ReturnT<String>(ReturnT.FAIL_CODE, e.getMessage());
				}
			}
		} else if (glueTypeEnum != null && glueTypeEnum.isScript()) {
			// 脚本触发
			// valid old jobThread
			if (jobThread != null &&
					!(jobThread.getHandler() instanceof ScriptJobHandler
							&& ((ScriptJobHandler) jobThread.getHandler()).getGlueUpdatetime() == triggerParam.getGlueUpdatetime())) {
				// change script or gluesource updated, need kill old thread
				removeOldReason = "change job source or glue type, and terminate the old job thread.";

				jobThread = null;
				jobHandler = null;
			}

			// valid handler
			if (jobHandler == null) {
				jobHandler = new ScriptJobHandler(triggerParam.getJobId(), triggerParam.getGlueUpdatetime(), triggerParam.getGlueSource(), GlueTypeEnum.match(triggerParam.getGlueType()));
			}
		} else {
			return new ReturnT<String>(ReturnT.FAIL_CODE, "glueType[" + triggerParam.getGlueType() + "] is not valid.");
		}

		// executor block strategy
		// 阻塞处理策略
		if (jobThread != null) {
			ExecutorBlockStrategyEnum blockStrategy = ExecutorBlockStrategyEnum.match(triggerParam.getExecutorBlockStrategy(), null);
			// 丢弃后续调度:调度请求进入单机执行器后,发现执行器存在运行的调度任务,本次请求将会被丢弃并标记为失败;
			if (ExecutorBlockStrategyEnum.DISCARD_LATER == blockStrategy) {
				// discard when running
				// 任务处于运行态或者有消息任务,则直接返回异常信息
				if (jobThread.isRunningOrHasQueue()) {
					return new ReturnT<String>(ReturnT.FAIL_CODE, "block strategy effect：" + ExecutorBlockStrategyEnum.DISCARD_LATER.getTitle());
				}
			} else if (ExecutorBlockStrategyEnum.COVER_EARLY == blockStrategy) {
				// 覆盖之前调度:请求进入单机执行器后,行器存在运行的调度任务,将会终止运行中的调度任务并清空队列,然后运行本地调度任务;
				// kill running jobThread
				if (jobThread.isRunningOrHasQueue()) {
					removeOldReason = "block strategy effect：" + ExecutorBlockStrategyEnum.COVER_EARLY.getTitle();
					// 释放线程引用
					jobThread = null;
				}
			} else {
				// just queue trigger
			}
		}

		// replace thread (new or exists invalid)
		if (jobThread == null) {
			// 创建执行控制器
			jobThread = XxlJobExecutor.registJobThread(triggerParam.getJobId(), jobHandler, removeOldReason);
		}

		// do push data to queue
		// 将数据放入执行队列
		// 作业没绑定过线程,则绑定作业到具体线程,并且启动
		ReturnT<String> pushResult = jobThread.pushTriggerQueue(triggerParam);
		return pushResult;
	}

	@Override
	public ReturnT<String> kill(KillParam killParam) {
		// kill handlerThread, and create new one
		JobThread jobThread = XxlJobExecutor.loadJobThread(killParam.getJobId());
		if (jobThread != null) {
			XxlJobExecutor.removeJobThread(killParam.getJobId(), "scheduling center kill job.");
			return ReturnT.SUCCESS;
		}

		return new ReturnT<String>(ReturnT.SUCCESS_CODE, "job thread already killed.");
	}

	@Override
	public ReturnT<LogResult> log(LogParam logParam) {
		// log filename: logPath/yyyy-MM-dd/9999.log
		// 获得日志文件名
		String logFileName = XxlJobFileAppender.makeLogFileName(new Date(logParam.getLogDateTim()), logParam.getLogId());
		// 读取流信息
		LogResult logResult = XxlJobFileAppender.readLog(logFileName, logParam.getFromLineNum());
		return new ReturnT<LogResult>(logResult);
	}

}
