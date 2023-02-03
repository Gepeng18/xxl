package com.xxl.rpc.core.remoting.provider;

import com.xxl.rpc.core.registry.Register;
import com.xxl.rpc.core.remoting.net.Server;
import com.xxl.rpc.core.remoting.net.impl.netty.server.NettyServer;
import com.xxl.rpc.core.remoting.net.params.BaseCallback;
import com.xxl.rpc.core.remoting.net.params.XxlRpcRequest;
import com.xxl.rpc.core.remoting.net.params.XxlRpcResponse;
import com.xxl.rpc.core.serialize.Serializer;
import com.xxl.rpc.core.serialize.impl.HessianSerializer;
import com.xxl.rpc.core.util.IpUtil;
import com.xxl.rpc.core.util.NetUtil;
import com.xxl.rpc.core.util.ThrowableUtil;
import com.xxl.rpc.core.util.XxlRpcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * provider
 *
 * @author xuxueli 2015-10-31 22:54:27
 */
public class XxlRpcProviderFactory {
	private static final Logger logger = LoggerFactory.getLogger(XxlRpcProviderFactory.class);

	// ---------------------- config ----------------------

	private Class<? extends Server> server = NettyServer.class;
	private Class<? extends Serializer> serializer = HessianSerializer.class;

	private int corePoolSize = 60;
	private int maxPoolSize = 300;

	private String ip = null;					// server ip, for registry
	private int port = 7080;					// server default port
	private String registryAddress;				// default use registryAddress to registry , otherwise use ip:port if registryAddress is null
	private String accessToken = null;

	private Class<? extends Register> serviceRegistry = null;
	private Map<String, String> serviceRegistryParam = null;

	// set
	public void setServer(Class<? extends Server> server) {
		this.server = server;
	}
	public void setSerializer(Class<? extends Serializer> serializer) {
		this.serializer = serializer;
	}
	public void setCorePoolSize(int corePoolSize) {
		this.corePoolSize = corePoolSize;
	}
	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public void setRegistryAddress(String registryAddress) {
		this.registryAddress = registryAddress;
	}
	public void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}
	public void setServiceRegistry(Class<? extends Register> serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
	}

	public void setServiceRegistryParam(Map<String, String> serviceRegistryParam) {
		this.serviceRegistryParam = serviceRegistryParam;
	}

	// get
	public Serializer getSerializerInstance() {
		return serializerInstance;
	}
	public int getPort() {
		return port;
	}
	public int getCorePoolSize() {
		return corePoolSize;
	}
	public int getMaxPoolSize() {
		return maxPoolSize;
	}

	// ---------------------- start / stop ----------------------

	private Server serverInstance;
	private Serializer serializerInstance;
	private Register registerInstance;  // 负责注册的

	public void start() throws Exception {

		// valid
		if (this.server == null) {
			throw new XxlRpcException("xxl-rpc provider server missing.");
		}
		if (this.serializer==null) {
			throw new XxlRpcException("xxl-rpc provider serializer missing.");
		}
		if (!(this.corePoolSize>0 && this.maxPoolSize>0 && this.maxPoolSize>=this.corePoolSize)) {
			this.corePoolSize = 60;
			this.maxPoolSize = 300;
		}
		if (this.ip == null) {
			this.ip = IpUtil.getIp();
		}
		if (this.port <= 0) {
			this.port = 7080;
		}
		if (this.registryAddress==null || this.registryAddress.trim().length()==0) {
			this.registryAddress = IpUtil.getIpPort(this.ip, this.port);
		}
		if (NetUtil.isPortUsed(this.port)) {
			throw new XxlRpcException("xxl-rpc provider port["+ this.port +"] is used.");
		}

		// XxlRpcProviderConfig 配置中只设置了一个类，这里进行初始化
		// init serializerInstance
		this.serializerInstance = serializer.newInstance();

		// start server
		serverInstance = server.newInstance();
		// 这里用了回调函数，而非观察者模式
		// 设置启动回调函数
		serverInstance.setStartedCallback(new BaseCallback() {		// serviceRegistry started
			@Override
			public void run() throws Exception {
				// start registry
				if (serviceRegistry != null) {
					registerInstance = serviceRegistry.newInstance();
					// XxlRpcProviderConfig 中添加了该参数，这里启动，即连接上注册中心
					registerInstance.start(serviceRegistryParam);
					if (serviceData.size() > 0) {
						// 将本地的service注册到注册中心
						registerInstance.registry(serviceData.keySet(), registryAddress);
					}
				}
			}
		});
		// 设置停止回调函数
		serverInstance.setStopedCallback(new BaseCallback() {		// serviceRegistry stoped
			@Override
			public void run() {
				// stop registry
				if (registerInstance != null) {
					if (serviceData.size() > 0) {
						// 该回调方法主要是将之前注册到注册中心的服务移除掉
						registerInstance.remove(serviceData.keySet(), registryAddress);
					}
					registerInstance.stop();
					registerInstance = null;
				}
			}
		});
		// 启动netty服务
		serverInstance.start(this);
	}

	public void  stop() throws Exception {
		// stop server
		serverInstance.stop();
	}


	// ---------------------- server invoke ----------------------

	/**
	 * init local rpc service map
	 */
	private Map<String, Object> serviceData = new HashMap<String, Object>();
	public Map<String, Object> getServiceData() {
		return serviceData;
	}

	/**
	 * make service key
	 *
	 * @param iface
	 * @param version
	 * @return
	 */
	public static String makeServiceKey(String iface, String version){
		String serviceKey = iface;
		if (version!=null && version.trim().length()>0) {
			serviceKey += "#".concat(version);
		}
		return serviceKey;
	}

	/**
	 * add service
	 *
	 * @param iface
	 * @param version
	 * @param serviceBean
	 */
	public void addService(String iface, String version, Object serviceBean){
		// 通过一定规则生成key
		String serviceKey = makeServiceKey(iface, version);
		// 将key 与带有注解的bean 存储在map中
		serviceData.put(serviceKey, serviceBean);

		logger.info(">>>>>>>>>>> xxl-rpc, provider factory add service success. serviceKey = {}, serviceBean = {}", serviceKey, serviceBean.getClass());
	}

	/**
	 * invoke service
	 *
	 * @param xxlRpcRequest
	 * @return
	 */
	public XxlRpcResponse invokeService(XxlRpcRequest xxlRpcRequest) {

		//  make response
		XxlRpcResponse xxlRpcResponse = new XxlRpcResponse();
		xxlRpcResponse.setRequestId(xxlRpcRequest.getRequestId());

		// match service bean
		// 从本地找到服务
		String serviceKey = makeServiceKey(xxlRpcRequest.getClassName(), xxlRpcRequest.getVersion());
		Object serviceBean = serviceData.get(serviceKey);

		// valid
		if (serviceBean == null) {
			xxlRpcResponse.setErrorMsg("The serviceKey["+ serviceKey +"] not found.");
			return xxlRpcResponse;
		}

		// 检测超时
		if (System.currentTimeMillis() - xxlRpcRequest.getCreateMillisTime() > 3*60*1000) {
			xxlRpcResponse.setErrorMsg("The timestamp difference between admin and executor exceeds the limit.");
			return xxlRpcResponse;
		}
		// 检测token
		if (accessToken!=null && accessToken.trim().length()>0 && !accessToken.trim().equals(xxlRpcRequest.getAccessToken())) {
			xxlRpcResponse.setErrorMsg("The access token[" + xxlRpcRequest.getAccessToken() + "] is wrong.");
			return xxlRpcResponse;
		}

		try {
			// invoke 反射调用
			Class<?> serviceClass = serviceBean.getClass();
			// 获取要执行的方法名称
			String methodName = xxlRpcRequest.getMethodName();
			//获取执行方法参数类型
			Class<?>[] parameterTypes = xxlRpcRequest.getParameterTypes();
			// 获取要执行方法参数值
			Object[] parameters = xxlRpcRequest.getParameters();

            Method method = serviceClass.getMethod(methodName, parameterTypes);
            method.setAccessible(true);
			Object result = method.invoke(serviceBean, parameters);

			/*FastClass serviceFastClass = FastClass.create(serviceClass);
			FastMethod serviceFastMethod = serviceFastClass.getMethod(methodName, parameterTypes);
			Object result = serviceFastMethod.invoke(serviceBean, parameters);*/

			xxlRpcResponse.setResult(result);
		} catch (Throwable t) {
			// catch error
			logger.error("xxl-rpc provider invokeService error.", t);
			xxlRpcResponse.setErrorMsg(ThrowableUtil.toString(t));
		}

		return xxlRpcResponse;
	}

}