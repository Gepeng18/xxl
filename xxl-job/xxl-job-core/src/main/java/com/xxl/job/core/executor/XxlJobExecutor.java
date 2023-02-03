package com.xxl.job.core.executor;

import com.xxl.job.core.biz.AdminBiz;
import com.xxl.job.core.biz.client.AdminBizClient;
import com.xxl.job.core.handler.IJobHandler;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.handler.impl.MethodJobHandler;
import com.xxl.job.core.log.XxlJobFileAppender;
import com.xxl.job.core.server.EmbedServer;
import com.xxl.job.core.thread.JobLogFileCleanThread;
import com.xxl.job.core.thread.JobThread;
import com.xxl.job.core.thread.TriggerCallbackThread;
import com.xxl.job.core.util.IpUtil;
import com.xxl.job.core.util.NetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Created by xuxueli on 2016/3/2 21:14.
 */
public class XxlJobExecutor {
	private static final Logger logger = LoggerFactory.getLogger(XxlJobExecutor.class);
	// ---------------------- admin-client (rpc invoker) ----------------------
	private static List<AdminBiz> adminBizList;
	// ---------------------- job handler repository ----------------------
	private static ConcurrentMap<String, IJobHandler> jobHandlerRepository = new ConcurrentHashMap<String, IJobHandler>();
	// ---------------------- job thread repository ----------------------
	private static ConcurrentMap<Integer, JobThread> jobThreadRepository = new ConcurrentHashMap<Integer, JobThread>();
	// ---------------------- param ----------------------
	private String adminAddresses;
	private String accessToken;
	private String appname;
	private String address;
	private String ip;
	private int port;
	private String logPath;
	private int logRetentionDays;
	// ---------------------- executor-server (rpc provider) ----------------------
	private EmbedServer embedServer = null;

	public static List<AdminBiz> getAdminBizList() {
		return adminBizList;
	}

	public static IJobHandler loadJobHandler(String name) {
		return jobHandlerRepository.get(name);
	}

	public static IJobHandler registJobHandler(String name, IJobHandler jobHandler) {
		logger.info(">>>>>>>>>>> xxl-job register jobhandler success, name:{}, jobHandler:{}", name, jobHandler);
		return jobHandlerRepository.put(name, jobHandler);
	}

	public static JobThread registJobThread(int jobId, IJobHandler handler, String removeOldReason) {
		JobThread newJobThread = new JobThread(jobId, handler);
		newJobThread.start();
		logger.info(">>>>>>>>>>> xxl-job regist JobThread success, jobId:{}, handler:{}", new Object[]{jobId, handler});
		// 存储jobId与绑定工作的线程
		JobThread oldJobThread = jobThreadRepository.put(jobId, newJobThread);    // putIfAbsent | oh my god, map's put method return the old value!!!
		if (oldJobThread != null) {
			// 中断并删除旧线程
			oldJobThread.toStop(removeOldReason);
			oldJobThread.interrupt();
		}

		return newJobThread;
	}

	public static JobThread removeJobThread(int jobId, String removeOldReason) {
		JobThread oldJobThread = jobThreadRepository.remove(jobId);
		if (oldJobThread != null) {
			oldJobThread.toStop(removeOldReason);
			oldJobThread.interrupt();

			return oldJobThread;
		}
		return null;
	}

	public static JobThread loadJobThread(int jobId) {
		return jobThreadRepository.get(jobId);
	}

	public void setAdminAddresses(String adminAddresses) {
		this.adminAddresses = adminAddresses;
	}

	public void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}

	public void setAppname(String appname) {
		this.appname = appname;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public void setIp(String ip) {
		this.ip = ip;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public void setLogPath(String logPath) {
		this.logPath = logPath;
	}

	public void setLogRetentionDays(int logRetentionDays) {
		this.logRetentionDays = logRetentionDays;
	}

	// ---------------------- start + stop ----------------------
	public void start() throws Exception {

		// init logpath
		// 初始化日志路径
		XxlJobFileAppender.initLogPath(logPath);

		// init invoker, admin-client
		// 初始化xxl-job连接信息
		initAdminBizList(adminAddresses, accessToken);


		// init JobLogFileCleanThread
		// 初始化日志文件清理线程
		JobLogFileCleanThread.getInstance().start(logRetentionDays);

		// init TriggerCallbackThread
		// 初始化回调线程
		TriggerCallbackThread.getInstance().start();

		// init executor-server
		// 启动netty服务，并注册至admin
		initEmbedServer(address, ip, port, appname, accessToken);
	}

	public void destroy() {
		// destroy executor-server
		stopEmbedServer();

		// destroy jobThreadRepository
		if (jobThreadRepository.size() > 0) {
			for (Map.Entry<Integer, JobThread> item : jobThreadRepository.entrySet()) {
				JobThread oldJobThread = removeJobThread(item.getKey(), "web container destroy and kill the job.");
				// wait for job thread push result to callback queue
				if (oldJobThread != null) {
					try {
						oldJobThread.join();
					} catch (InterruptedException e) {
						logger.error(">>>>>>>>>>> xxl-job, JobThread destroy(join) error, jobId:{}", item.getKey(), e);
					}
				}
			}
			jobThreadRepository.clear();
		}
		jobHandlerRepository.clear();


		// destroy JobLogFileCleanThread
		JobLogFileCleanThread.getInstance().toStop();

		// destroy TriggerCallbackThread
		TriggerCallbackThread.getInstance().toStop();

	}

	// 当存在多个任务调度中心时，创建多个AdminBizClient(adminAddress)
	private void initAdminBizList(String adminAddresses, String accessToken) throws Exception {
		if (adminAddresses != null && adminAddresses.trim().length() > 0) {
			for (String address : adminAddresses.trim().split(",")) {
				if (address != null && address.trim().length() > 0) {

					AdminBiz adminBiz = new AdminBizClient(address.trim(), accessToken);

					if (adminBizList == null) {
						adminBizList = new ArrayList<AdminBiz>();
					}
					// 将admin地址以及token添加adminBiz中
					adminBizList.add(adminBiz);
				}
			}
		}
	}

	/**
	 * 1. 使用netty开放端口，等待服务端调用
	 * 2. 注册到服务端(心跳30S)
	 * 3. 向服务端申请剔除服务
	 */
	private void initEmbedServer(String address, String ip, int port, String appname, String accessToken) throws Exception {

		// fill ip port
		// 选择可用端口号，从9999向下找到未使用过的端口
		port = port > 0 ? port : NetUtil.findAvailablePort(9999);
		// 获得本机内网ip地址
		ip = (ip != null && ip.trim().length() > 0) ? ip : IpUtil.getIp();

		// generate address
		// 拼接应用请求地址
		if (address == null || address.trim().length() == 0) {
			String ip_port_address = IpUtil.getIpPort(ip, port);   // registry-address：default use address to registry , otherwise use ip:port if address is null
			address = "http://{ip_port}/".replace("{ip_port}", ip_port_address);
		}

		// accessToken
		if (accessToken == null || accessToken.trim().length() == 0) {
			logger.warn(">>>>>>>>>>> xxl-job accessToken is empty. To ensure system security, please set the accessToken.");
		}

		// start
		// 创建server实例并启动,启动嵌入服务器 ,向服务端注册,以及监听端口,主要服务服务端调用。
		embedServer = new EmbedServer();
		embedServer.start(address, port, appname, accessToken);
	}

	private void stopEmbedServer() {
		// stop provider factory
		if (embedServer != null) {
			try {
				embedServer.stop();
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
	}

	protected void registJobHandler(XxlJob xxlJob, Object bean, Method executeMethod) {
		if (xxlJob == null) {
			return;
		}

		String name = xxlJob.value();
		//make and simplify the variables since they'll be called several times later
		Class<?> clazz = bean.getClass();
		String methodName = executeMethod.getName();
		if (name.trim().length() == 0) {
			throw new RuntimeException("xxl-job method-jobhandler name invalid, for[" + clazz + "#" + methodName + "] .");
		}
		if (loadJobHandler(name) != null) {
			throw new RuntimeException("xxl-job jobhandler[" + name + "] naming conflicts.");
		}

		// execute method
        /*if (!(method.getParameterTypes().length == 1 && method.getParameterTypes()[0].isAssignableFrom(String.class))) {
            throw new RuntimeException("xxl-job method-jobhandler param-classtype invalid, for[" + bean.getClass() + "#" + method.getName() + "] , " +
                    "The correct method format like \" public ReturnT<String> execute(String param) \" .");
        }
        if (!method.getReturnType().isAssignableFrom(ReturnT.class)) {
            throw new RuntimeException("xxl-job method-jobhandler return-classtype invalid, for[" + bean.getClass() + "#" + method.getName() + "] , " +
                    "The correct method format like \" public ReturnT<String> execute(String param) \" .");
        }*/

		executeMethod.setAccessible(true);

		// init and destroy
		Method initMethod = null;
		Method destroyMethod = null;

		if (xxlJob.init().trim().length() > 0) {
			try {
				initMethod = clazz.getDeclaredMethod(xxlJob.init());
				initMethod.setAccessible(true);
			} catch (NoSuchMethodException e) {
				throw new RuntimeException("xxl-job method-jobhandler initMethod invalid, for[" + clazz + "#" + methodName + "] .");
			}
		}
		if (xxlJob.destroy().trim().length() > 0) {
			try {
				destroyMethod = clazz.getDeclaredMethod(xxlJob.destroy());
				destroyMethod.setAccessible(true);
			} catch (NoSuchMethodException e) {
				throw new RuntimeException("xxl-job method-jobhandler destroyMethod invalid, for[" + clazz + "#" + methodName + "] .");
			}
		}

		// registry jobhandler
		registJobHandler(name, new MethodJobHandler(bean, executeMethod, initMethod, destroyMethod));

	}
}
