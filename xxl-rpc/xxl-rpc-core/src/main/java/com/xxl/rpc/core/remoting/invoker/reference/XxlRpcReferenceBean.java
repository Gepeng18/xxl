package com.xxl.rpc.core.remoting.invoker.reference;

import com.xxl.rpc.core.remoting.invoker.XxlRpcInvokerFactory;
import com.xxl.rpc.core.remoting.invoker.call.CallType;
import com.xxl.rpc.core.remoting.invoker.call.XxlRpcInvokeCallback;
import com.xxl.rpc.core.remoting.invoker.call.XxlRpcInvokeFuture;
import com.xxl.rpc.core.remoting.invoker.generic.XxlRpcGenericService;
import com.xxl.rpc.core.remoting.invoker.route.LoadBalance;
import com.xxl.rpc.core.remoting.net.Client;
import com.xxl.rpc.core.remoting.net.impl.netty.client.NettyClient;
import com.xxl.rpc.core.remoting.net.params.XxlRpcFutureResponse;
import com.xxl.rpc.core.remoting.net.params.XxlRpcRequest;
import com.xxl.rpc.core.remoting.net.params.XxlRpcResponse;
import com.xxl.rpc.core.remoting.provider.XxlRpcProviderFactory;
import com.xxl.rpc.core.serialize.Serializer;
import com.xxl.rpc.core.serialize.impl.HessianSerializer;
import com.xxl.rpc.core.util.ClassUtil;
import com.xxl.rpc.core.util.XxlRpcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * rpc reference bean, use by api
 *
 * @author xuxueli 2015-10-29 20:18:32
 */
public class XxlRpcReferenceBean {
	private static final Logger logger = LoggerFactory.getLogger(XxlRpcReferenceBean.class);
	// [tips01: save 30ms/100invoke. why why why??? with this logger, it can save lots of time.]


	// ---------------------- config ----------------------

	private Class<? extends Client> client = NettyClient.class;
	private Class<? extends Serializer> serializer = HessianSerializer.class;  // 序列化方式，hession
	private CallType callType = CallType.SYNC;                  // 调用方式
	private LoadBalance loadBalance = LoadBalance.ROUND;    //负载均衡策略

	private Class<?> iface = null;       // 接口class对象
	private String version = null;       // 版本

	private long timeout = 1000;         //超时时间

	private String address = null;       // 服务提供者地址
	private String accessToken = null;   // token

	private XxlRpcInvokeCallback invokeCallback = null;

	private XxlRpcInvokerFactory invokerFactory = null;


	// set
	public void setClient(Class<? extends Client> client) {
		this.client = client;
	}
	public void setSerializer(Class<? extends Serializer> serializer) {
		this.serializer = serializer;
	}
	public void setCallType(CallType callType) {
		this.callType = callType;
	}
	public void setLoadBalance(LoadBalance loadBalance) {
		this.loadBalance = loadBalance;
	}
	public void setIface(Class<?> iface) {
		this.iface = iface;
	}
	public void setVersion(String version) {
		this.version = version;
	}
	public void setTimeout(long timeout) {
		this.timeout = timeout;
	}
	public void setAddress(String address) {
		this.address = address;
	}
	public void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}
	public void setInvokeCallback(XxlRpcInvokeCallback invokeCallback) {
		this.invokeCallback = invokeCallback;
	}
	public void setInvokerFactory(XxlRpcInvokerFactory invokerFactory) {
		this.invokerFactory = invokerFactory;
	}


	// get
	public Serializer getSerializerInstance() {
		return serializerInstance;
	}
	public long getTimeout() {
		return timeout;
	}

	public XxlRpcInvokerFactory getInvokerFactory() {
		return invokerFactory;
	}
	public Class<?> getIface() {
		return iface;
	}


	// ---------------------- initClient ----------------------

	private Client clientInstance = null;
	private Serializer serializerInstance = null;

	public XxlRpcReferenceBean initClient() throws Exception {

		// valid
		if (this.client == null) {
			throw new XxlRpcException("xxl-rpc reference client missing.");
		}
		if (this.serializer == null) {
			throw new XxlRpcException("xxl-rpc reference serializer missing.");
		}
		if (this.callType==null) {
			throw new XxlRpcException("xxl-rpc reference callType missing.");
		}
		if (this.loadBalance==null) {
			throw new XxlRpcException("xxl-rpc reference loadBalance missing.");
		}
		if (this.iface==null) {
			throw new XxlRpcException("xxl-rpc reference iface missing.");
		}
		if (this.timeout < 0) {
			this.timeout = 0;
		}
		if (this.invokerFactory == null) {
			this.invokerFactory = XxlRpcInvokerFactory.getInstance();
		}

		// init serializerInstance
		this.serializerInstance = serializer.newInstance();

		// init Client
		clientInstance = client.newInstance();
		clientInstance.init(this);

		return this;
	}


	// ---------------------- util ----------------------

	/**
	 * 这个方法虽然看起来非常的长，但是也不是很麻烦，主要干了这些事情：
	 * 1、获取请求的一些参数，包括你要调哪个类，哪个方法，方法参数类型，方法参数值。
	 * 2、如果是泛化调用（你自己指定1里面的字段）类，方法名，方法参数类型，方法参数 就要按照你指定的来。
	 * 3、服务提供者地址的选择，根据负载均衡策略来，你自己在注解中配置的优先级最低（这个有点想不通）。
	 * 4、封装请求参数，将类，方法名，方法参数类型，方法参数值，还有一个唯一的uuid ，封装成Request对象实体。
	 * 5、根据调用策略来进行调用。
	 */
	public Object getObject() throws Exception {

		// initClient
		initClient();

		// newProxyInstance
		return Proxy.newProxyInstance(Thread.currentThread()
				.getContextClassLoader(), new Class[] { iface },
				new InvocationHandler() {
					@Override
					public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

						// method param 生成一些 调用参数
						String className = method.getDeclaringClass().getName();	// iface.getName()
						String varsion_ = version;
						// 方法名
						String methodName = method.getName();
						// 方法参数类型
						Class<?>[] parameterTypes = method.getParameterTypes();
						// 方法参数值
						Object[] parameters = args;

						// filter for generic
						// 这个就是判断是否是泛化调用，如果是泛化调用，参数就按照用户配置的来，比如说 你要调用哪个类，方法名，参数类型，参数值。
						// 这些都要设定好
						if (className.equals(XxlRpcGenericService.class.getName()) && methodName.equals("invoke")) {

							Class<?>[] paramTypes = null;
							if (args[3]!=null) {
								String[] paramTypes_str = (String[]) args[3];
								if (paramTypes_str.length > 0) {
									paramTypes = new Class[paramTypes_str.length];
									for (int i = 0; i < paramTypes_str.length; i++) {
										paramTypes[i] = ClassUtil.resolveClass(paramTypes_str[i]);
									}
								}
							}

							className = (String) args[0];
							varsion_ = (String) args[1];
							methodName = (String) args[2];
							parameterTypes = paramTypes;
							parameters = (Object[]) args[4];
						}

						// 这边就是确认一下 你这方法是出自哪个类的
						// filter method like "Object.toString()"
						if (className.equals(Object.class.getName())) {
							logger.info(">>>>>>>>>>> xxl-rpc proxy class-method not support [{}#{}]", className, methodName);
							throw new XxlRpcException("xxl-rpc proxy class-method not support");
						}

						// address
						// 服务提供者 地址的选择， 根据负载均衡策略来选择
						String finalAddress = address;
						if (finalAddress==null || finalAddress.trim().length()==0) {
							if (invokerFactory!=null && invokerFactory.getRegister()!=null) {
								// discovery
								String serviceKey = XxlRpcProviderFactory.makeServiceKey(className, varsion_);
								TreeSet<String> addressSet = invokerFactory.getRegister().discovery(serviceKey);
								// load balance
								if (addressSet==null || addressSet.size()==0) {
									// pass
								} else if (addressSet.size()==1) {
									// 就一个的时候，那就选第一个
									finalAddress = addressSet.first();
								} else {
									// 负载均衡
									finalAddress = loadBalance.xxlRpcInvokerRouter.route(serviceKey, addressSet);
								}

							}
						}
						// 最后调用地址还是 null 就抛出异常了
						if (finalAddress==null || finalAddress.trim().length()==0) {
							throw new XxlRpcException("xxl-rpc reference bean["+ className +"] address empty");
						}

						// request
						// request 请求参数封装
						XxlRpcRequest xxlRpcRequest = new XxlRpcRequest();
	                    xxlRpcRequest.setRequestId(UUID.randomUUID().toString());
	                    xxlRpcRequest.setCreateMillisTime(System.currentTimeMillis());
	                    xxlRpcRequest.setAccessToken(accessToken);
	                    xxlRpcRequest.setClassName(className);
	                    xxlRpcRequest.setMethodName(methodName);
	                    xxlRpcRequest.setParameterTypes(parameterTypes);
	                    xxlRpcRequest.setParameters(parameters);
	                    xxlRpcRequest.setVersion(version);
	                    
	                    // send
						// send  根据调用策略 调用
						if (CallType.SYNC == callType) {
							// future-response set
							// 创建一个futureResponse，放入本地池中（key为requestId，value是futureResponse）
							XxlRpcFutureResponse futureResponse = new XxlRpcFutureResponse(invokerFactory, xxlRpcRequest, null);
							try {
								// do invoke
								clientInstance.asyncSend(finalAddress, xxlRpcRequest);

								// future get
								XxlRpcResponse xxlRpcResponse = futureResponse.get(timeout, TimeUnit.MILLISECONDS);
								if (xxlRpcResponse.getErrorMsg() != null) {
									throw new XxlRpcException(xxlRpcResponse.getErrorMsg());
								}
								return xxlRpcResponse.getResult();
							} catch (Exception e) {
								logger.info(">>>>>>>>>>> xxl-rpc, invoke error, address:{}, XxlRpcRequest{}", finalAddress, xxlRpcRequest);

								throw (e instanceof XxlRpcException)?e:new XxlRpcException(e);
							} finally{
								// future-response remove
								futureResponse.removeInvokerFuture();
							}
						} else if (CallType.FUTURE == callType) {
							// future-response set
							// 创建一个futureResponse，放入本地池中（key为requestId，value是futureResponse）
							XxlRpcFutureResponse futureResponse = new XxlRpcFutureResponse(invokerFactory, xxlRpcRequest, null);
                            try {
								// invoke future set
								XxlRpcInvokeFuture invokeFuture = new XxlRpcInvokeFuture(futureResponse);
								// 将futureResponse放入threadLocal中
								XxlRpcInvokeFuture.setFuture(invokeFuture);

                                // do invoke
								clientInstance.asyncSend(finalAddress, xxlRpcRequest);

                                return null;
                            } catch (Exception e) {
								logger.info(">>>>>>>>>>> xxl-rpc, invoke error, address:{}, XxlRpcRequest{}", finalAddress, xxlRpcRequest);

								// future-response remove
								futureResponse.removeInvokerFuture();

								throw (e instanceof XxlRpcException)?e:new XxlRpcException(e);
                            }

						} else if (CallType.CALLBACK == callType) {

							// get callback
							XxlRpcInvokeCallback finalInvokeCallback = invokeCallback;
							XxlRpcInvokeCallback threadInvokeCallback = XxlRpcInvokeCallback.getCallback();
							if (threadInvokeCallback != null) {
								finalInvokeCallback = threadInvokeCallback;
							}
							if (finalInvokeCallback == null) {
								throw new XxlRpcException("xxl-rpc XxlRpcInvokeCallback（CallType="+ CallType.CALLBACK.name() +"） cannot be null.");
							}

							// future-response set
							XxlRpcFutureResponse futureResponse = new XxlRpcFutureResponse(invokerFactory, xxlRpcRequest, finalInvokeCallback);
							try {
								clientInstance.asyncSend(finalAddress, xxlRpcRequest);
							} catch (Exception e) {
								logger.info(">>>>>>>>>>> xxl-rpc, invoke error, address:{}, XxlRpcRequest{}", finalAddress, xxlRpcRequest);

								// future-response remove
								futureResponse.removeInvokerFuture();

								throw (e instanceof XxlRpcException)?e:new XxlRpcException(e);
							}

							return null;
						} else if (CallType.ONEWAY == callType) {
							clientInstance.asyncSend(finalAddress, xxlRpcRequest);
                            return null;
                        } else {
							throw new XxlRpcException("xxl-rpc callType["+ callType +"] invalid");
						}

					}
				});
	}

}
