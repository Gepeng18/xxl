package com.xxl.job.core.thread;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.model.RegistryParam;
import com.xxl.job.core.biz.model.ReturnT;
import com.xxl.job.core.enums.RegistryConfig;
import com.xxl.job.core.executor.XxlJobExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by xuxueli on 17/3/2.
 */
public class ExecutorRegistryThread {
	private static Logger logger = LoggerFactory.getLogger(ExecutorRegistryThread.class);

	private static ExecutorRegistryThread instance = new ExecutorRegistryThread();
	private Thread registryThread;
	private volatile boolean toStop = false;

	public static ExecutorRegistryThread getInstance() {
		return instance;
	}

	/**
	 * 以上代码为服务注册，也是心跳机制（api/registry），当服务停止后stop变量会变为true，这时会通知服务端下线（api/registryRemove），
	 * 如果服务突然宕机也没有关系，服务端有循环检查机制，当长时间没有收到客户端的心跳，会自动下线。
	 */
	public void start(final String appname, final String address) {

		// valid，appname不允许为null
		if (appname == null || appname.trim().length() == 0) {
			logger.warn(">>>>>>>>>>> xxl-job, executor registry config fail, appname is null.");
			return;
		}
		// 服务端地址不能为null
		if (XxlJobExecutor.getAdminBizList() == null) {
			logger.warn(">>>>>>>>>>> xxl-job, executor registry config fail, adminAddresses is null.");
			return;
		}

		registryThread = new Thread(new Runnable() {
			@Override
			public void run() {
				// registry
				// 循环心跳机制
				while (!toStop) {
					try {
						// 构建请求参数
						RegistryParam registryParam = new RegistryParam(RegistryConfig.RegistType.EXECUTOR.name(), appname, address);
						for (AdminBiz adminBiz : XxlJobExecutor.getAdminBizList()) {
							try {
								// 服务注册/心跳
								// 向server注册服务(http请求),注册内容appname,当前服务监听地址
								ReturnT<String> registryResult = adminBiz.registry(registryParam);
								// 访问成功
								if (registryResult != null && ReturnT.SUCCESS_CODE == registryResult.getCode()) {
									registryResult = ReturnT.SUCCESS;
									logger.debug(">>>>>>>>>>> xxl-job registry success, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
									break;
								} else {
									logger.info(">>>>>>>>>>> xxl-job registry fail, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
								}
							} catch (Exception e) {
								logger.info(">>>>>>>>>>> xxl-job registry error, registryParam:{}", registryParam, e);
							}

						}
					} catch (Exception e) {
						if (!toStop) {

							logger.error(e.getMessage(), e);
						}

					}

					try {
						if (!toStop) {
							// 心跳时长 30秒
							TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
						}
					} catch (InterruptedException e) {
						if (!toStop) {
							logger.warn(">>>>>>>>>>> xxl-job, executor registry thread interrupted, error msg:{}", e.getMessage());
						}
					}
				}

				// 当服务停止后，代码就执行到这里了，然后删除注册
				// registry remove
				// 服务停止后以下代码服务下线通知
				try {
					RegistryParam registryParam = new RegistryParam(RegistryConfig.RegistType.EXECUTOR.name(), appname, address);
					for (AdminBiz adminBiz : XxlJobExecutor.getAdminBizList()) {
						try {
							ReturnT<String> registryResult = adminBiz.registryRemove(registryParam);
							if (registryResult != null && ReturnT.SUCCESS_CODE == registryResult.getCode()) {
								registryResult = ReturnT.SUCCESS;
								logger.info(">>>>>>>>>>> xxl-job registry-remove success, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
								break;
							} else {
								logger.info(">>>>>>>>>>> xxl-job registry-remove fail, registryParam:{}, registryResult:{}", new Object[]{registryParam, registryResult});
							}
						} catch (Exception e) {
							if (!toStop) {
								logger.info(">>>>>>>>>>> xxl-job registry-remove error, registryParam:{}", registryParam, e);
							}

						}

					}
				} catch (Exception e) {
					if (!toStop) {
						logger.error(e.getMessage(), e);
					}
				}
				logger.info(">>>>>>>>>>> xxl-job, executor registry thread destroy.");

			}
		});
		registryThread.setDaemon(true);
		registryThread.setName("xxl-job, executor ExecutorRegistryThread");
		registryThread.start();
	}

	public void toStop() {
		toStop = true;

		// interrupt and wait
		if (registryThread != null) {
			registryThread.interrupt();
			try {
				registryThread.join();
			} catch (InterruptedException e) {
				logger.error(e.getMessage(), e);
			}
		}

	}

}
