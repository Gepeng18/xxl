package com.xxl.rpc.core.remoting.invoker;

import com.xxl.rpc.core.registry.Register;
import com.xxl.rpc.core.registry.impl.LocalRegister;
import com.xxl.rpc.core.remoting.net.params.BaseCallback;
import com.xxl.rpc.core.remoting.net.params.XxlRpcFutureResponse;
import com.xxl.rpc.core.remoting.net.params.XxlRpcResponse;
import com.xxl.rpc.core.util.XxlRpcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * xxl-rpc invoker factory, init service-registry
 * 主要是做了服务注册，客户端线程池响应初始化和回调线程池的初始化，客户端的 start，stop的工作。
 *
 * @author xuxueli 2018-10-19
 */
public class XxlRpcInvokerFactory {
	private static Logger logger = LoggerFactory.getLogger(XxlRpcInvokerFactory.class);

	// ---------------------- default instance ----------------------

	private static volatile XxlRpcInvokerFactory instance = new XxlRpcInvokerFactory(LocalRegister.class, null);

	public static XxlRpcInvokerFactory getInstance() {
		return instance;
	}


	// ---------------------- config ----------------------

	private Class<? extends Register> serviceRegistryClass;          // class.forname
	private Map<String, String> serviceRegistryParam;


	public XxlRpcInvokerFactory() {
	}

	public XxlRpcInvokerFactory(Class<? extends Register> serviceRegistryClass, Map<String, String> serviceRegistryParam) {
		this.serviceRegistryClass = serviceRegistryClass;
		this.serviceRegistryParam = serviceRegistryParam;
	}


	// ---------------------- start / stop ----------------------

	public void start() throws Exception {
		// start registry
		if (serviceRegistryClass != null) {
			// 创建 注册中心 对象
			register = serviceRegistryClass.newInstance();
			// 创建注册中心client并创建守护线程来刷新获取服务提供者列表。
			register.start(serviceRegistryParam);
		}
	}

	public void stop() throws Exception {
		// stop registry  停止注册中心客户端
		if (register != null) {
			register.stop();
		}

		// stop callback  执行停止回调方法
		if (stopCallbackList.size() > 0) {
			for (BaseCallback callback : stopCallbackList) {
				try {
					callback.run();
				} catch (Exception e) {
					logger.error(e.getMessage(), e);
				}
			}
		}

		// stop CallbackThreadPool  关闭线程池
		stopCallbackThreadPool();
	}


	// ---------------------- service registry ----------------------

	private Register register;

	public Register getRegister() {
		return register;
	}


	// ---------------------- service registry ----------------------

	private List<BaseCallback> stopCallbackList = new ArrayList<BaseCallback>();

	public void addStopCallBack(BaseCallback callback) {
		stopCallbackList.add(callback);
	}


	// ---------------------- future-response pool ----------------------

	// XxlRpcFutureResponseFactory

	private ConcurrentMap<String, XxlRpcFutureResponse> futureResponsePool = new ConcurrentHashMap<String, XxlRpcFutureResponse>();

	public void setInvokerFuture(String requestId, XxlRpcFutureResponse futureResponse) {
		futureResponsePool.put(requestId, futureResponse);
	}

	public void removeInvokerFuture(String requestId) {
		futureResponsePool.remove(requestId);
	}

	public void notifyInvokerFuture(String requestId, final XxlRpcResponse xxlRpcResponse) {

		// get
		final XxlRpcFutureResponse futureResponse = futureResponsePool.get(requestId);
		if (futureResponse == null) {
			return;
		}

		// notify
		if (futureResponse.getInvokeCallback() != null) {

			// callback type
			try {
				executeResponseCallback(new Runnable() {
					@Override
					public void run() {
						if (xxlRpcResponse.getErrorMsg() != null) {
							futureResponse.getInvokeCallback().onFailure(new XxlRpcException(xxlRpcResponse.getErrorMsg()));
						} else {
							futureResponse.getInvokeCallback().onSuccess(xxlRpcResponse.getResult());
						}
					}
				});
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		} else {

			// other nomal type
			futureResponse.setResponse(xxlRpcResponse);
		}

		// do remove
		futureResponsePool.remove(requestId);

	}


	// ---------------------- response callback ThreadPool ----------------------

	private ThreadPoolExecutor responseCallbackThreadPool = null;

	public void executeResponseCallback(Runnable runnable) {

		if (responseCallbackThreadPool == null) {
			synchronized (this) {
				if (responseCallbackThreadPool == null) {
					responseCallbackThreadPool = new ThreadPoolExecutor(
							10,
							100,
							60L,
							TimeUnit.SECONDS,
							new LinkedBlockingQueue<Runnable>(1000),
							new ThreadFactory() {
								@Override
								public Thread newThread(Runnable r) {
									return new Thread(r, "xxl-rpc, XxlRpcInvokerFactory-responseCallbackThreadPool-" + r.hashCode());
								}
							},
							new RejectedExecutionHandler() {
								@Override
								public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
									throw new XxlRpcException("xxl-rpc Invoke Callback Thread pool is EXHAUSTED!");
								}
							});        // default maxThreads 300, minThreads 60
				}
			}
		}
		responseCallbackThreadPool.execute(runnable);
	}

	public void stopCallbackThreadPool() {
		if (responseCallbackThreadPool != null) {
			responseCallbackThreadPool.shutdown();
		}
	}

}
