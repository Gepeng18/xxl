package com.xxl.mq.client.factory;

import com.xxl.mq.client.broker.IXxlMqBroker;
import com.xxl.mq.client.consumer.IMqConsumer;
import com.xxl.mq.client.consumer.annotation.MqConsumer;
import com.xxl.mq.client.consumer.registry.ConsumerRegistryHelper;
import com.xxl.mq.client.consumer.thread.ConsumerThread;
import com.xxl.mq.client.message.XxlMqMessage;
import com.xxl.rpc.registry.impl.XxlRegistryServiceRegistry;
import com.xxl.rpc.remoting.invoker.XxlRpcInvokerFactory;
import com.xxl.rpc.remoting.invoker.call.CallType;
import com.xxl.rpc.remoting.invoker.reference.XxlRpcReferenceBean;
import com.xxl.rpc.remoting.invoker.route.LoadBalance;
import com.xxl.rpc.remoting.net.NetEnum;
import com.xxl.rpc.serialize.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author xuxueli 2018-11-18 21:18:10
 */
public class XxlMqClientFactory {
	private final static Logger logger = LoggerFactory.getLogger(XxlMqClientFactory.class);


	// ---------------------- param  ----------------------
	public static volatile boolean clientFactoryPoolStoped = false;
	private static IXxlMqBroker xxlMqBroker;
	private static ConsumerRegistryHelper consumerRegistryHelper = null;
	private static LinkedBlockingQueue<XxlMqMessage> newMessageQueue = new LinkedBlockingQueue<>();
	private static LinkedBlockingQueue<XxlMqMessage> callbackMessageQueue = new LinkedBlockingQueue<>();
	private String adminAddress;

	// ---------------------- init destroy  ----------------------
	private String accessToken;
	private List<IMqConsumer> consumerList;


	// ---------------------- thread pool ----------------------
	private ExecutorService clientFactoryThreadPool = Executors.newCachedThreadPool();
	private XxlRpcInvokerFactory xxlRpcInvokerFactory = null;
	// queue consumer respository
	private List<ConsumerThread> consumerRespository = new ArrayList<ConsumerThread>();


	// ---------------------- broker service ----------------------

	public static IXxlMqBroker getXxlMqBroker() {
		return xxlMqBroker;
	}

	public static ConsumerRegistryHelper getConsumerRegistryHelper() {
		return consumerRegistryHelper;
	}

	public static void addMessages(XxlMqMessage mqMessage, boolean async) {
		if (async) {
			// ?????????????????????????????????
			// async queue, multi send
			newMessageQueue.add(mqMessage);
		} else {
			// ?????????????????????broker??????????????????
			// sync rpc, one send
			xxlMqBroker.addMessages(Arrays.asList(mqMessage));
		}

	}

	public static void callbackMessage(XxlMqMessage mqMessage) {
		callbackMessageQueue.add(mqMessage);
	}

	public void setAdminAddress(String adminAddress) {
		this.adminAddress = adminAddress;
	}

	public void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}

	public void setConsumerList(List<IMqConsumer> consumerList) {
		this.consumerList = consumerList;
	}

	public void init() {

		// pre : valid consumer
		// xxlMqClientFactory.setConsumerList(consumerList) ???????????????spring??????MqConsumer?????????consumerList???
		// 1????????????????????????consumerList??????consumer?????????ConsumerThread??????consumerRespository???
		validConsumer();


		// start BrokerService
		// 2????????????????????????????????????MessageQueue????????????????????????????????????xxlMqBroker????????????
		startBrokerService();

		// start consumer
		// 3??????consumer?????????????????????????????????????????????ConsumerThread
		startConsumer();

	}

	public void destroy() throws Exception {

		// pre : destory ClientFactoryThreadPool
		destoryClientFactoryThreadPool();


		// destory Consumer
		destoryConsumer();

		// destory BrokerService
		destoryBrokerService();

	}

	/**
	 * destory consumer thread
	 */
	private void destoryClientFactoryThreadPool() {
		clientFactoryPoolStoped = true;
		clientFactoryThreadPool.shutdownNow();
	}

	/**
	 * 1???????????????????????????
	 * 2????????????????????????????????????MessageQueue????????????????????????????????????xxlMqBroker????????????
	 */
	public void startBrokerService() {
		// init XxlRpcInvokerFactory
		// 1????????????????????????????????????
		xxlRpcInvokerFactory = new XxlRpcInvokerFactory(XxlRegistryServiceRegistry.class, new HashMap<String, String>() {{
			put(XxlRegistryServiceRegistry.XXL_REGISTRY_ADDRESS, adminAddress);
			put(XxlRegistryServiceRegistry.ACCESS_TOKEN, accessToken);
		}});
		try {
			xxlRpcInvokerFactory.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		// init ConsumerRegistryHelper
		// 2????????????????????????ConsumerRegistryHelper???
		XxlRegistryServiceRegistry commonServiceRegistry = (XxlRegistryServiceRegistry) xxlRpcInvokerFactory.getServiceRegistry();
		consumerRegistryHelper = new ConsumerRegistryHelper(commonServiceRegistry);

		// do ?????????????????????
		// init IXxlMqBroker
		// 3???????????????IXxlMqBroker????????????
		xxlMqBroker = (IXxlMqBroker) new XxlRpcReferenceBean(
				NetEnum.NETTY,
				Serializer.SerializeEnum.HESSIAN.getSerializer(),
				CallType.SYNC,
				LoadBalance.ROUND,
				IXxlMqBroker.class,
				null,
				10000,
				null,
				null,
				null,
				xxlRpcInvokerFactory).getObject();

		// async + multi, addMessages
		for (int i = 0; i < 3; i++) {
			clientFactoryThreadPool.execute(new Runnable() {
				@Override
				public void run() {

					while (!XxlMqClientFactory.clientFactoryPoolStoped) {
						try {
							// 4??????newMessageQueue?????????????????????
							XxlMqMessage message = newMessageQueue.take();
							if (message != null) {
								// load
								List<XxlMqMessage> messageList = new ArrayList<>();
								messageList.add(message);

								// ?????????100???????????????????????????
								List<XxlMqMessage> otherMessageList = new ArrayList<>();
								int drainToNum = newMessageQueue.drainTo(otherMessageList, 100);
								if (drainToNum > 0) {
									messageList.addAll(otherMessageList);
								}

								// ??????rpc???????????????????????????broker
								xxlMqBroker.addMessages(messageList);
							}
						} catch (Exception e) {
							if (!XxlMqClientFactory.clientFactoryPoolStoped) {
								logger.error(e.getMessage(), e);
							}
						}
					}

					// ????????????????????????????????????????????????newMessageQueue???????????????????????????????????????xxlMqBroker
					// finally total
					List<XxlMqMessage> otherMessageList = new ArrayList<>();
					int drainToNum = newMessageQueue.drainTo(otherMessageList);
					if (drainToNum > 0) {
						xxlMqBroker.addMessages(otherMessageList);
					}

				}
			});
		}

		// async + multi, addMessages
		for (int i = 0; i < 3; i++) {
			clientFactoryThreadPool.execute(new Runnable() {
				@Override
				public void run() {
					while (!XxlMqClientFactory.clientFactoryPoolStoped) {
						try {
							// 1??????callbackMessageQueue???????????????
							XxlMqMessage message = callbackMessageQueue.take();
							if (message != null) {
								// load
								List<XxlMqMessage> messageList = new ArrayList<>();
								messageList.add(message);

								List<XxlMqMessage> otherMessageList = new ArrayList<>();
								int drainToNum = callbackMessageQueue.drainTo(otherMessageList, 100);
								if (drainToNum > 0) {
									messageList.addAll(otherMessageList);
								}

								// callback
								// ??????xxlMqBroker.callbackMessages
								xxlMqBroker.callbackMessages(messageList);
							}
						} catch (Exception e) {
							if (!XxlMqClientFactory.clientFactoryPoolStoped) {
								logger.error(e.getMessage(), e);
							}
						}
					}

					// finally total
					List<XxlMqMessage> otherMessageList = new ArrayList<>();
					int drainToNum = callbackMessageQueue.drainTo(otherMessageList);
					if (drainToNum > 0) {
						xxlMqBroker.callbackMessages(otherMessageList);
					}

				}
			});
		}


	}


	// ---------------------- queue consumer ----------------------

	public void destoryBrokerService() throws Exception {
		// stop invoker factory
		if (xxlRpcInvokerFactory != null) {
			xxlRpcInvokerFactory.stop();
		}
	}

	/**
	 * 1?????????MqConsumer????????????group???null???????????????uuid
	 * 2????????????MqConsumer?????????????????????List???
	 */
	private void validConsumer() {
		// valid
		if (consumerList == null || consumerList.size() == 0) {
			logger.warn(">>>>>>>>>>> xxl-mq, MqConsumer not found.");
			return;
		}

		// make ConsumerThread
		for (IMqConsumer consumer : consumerList) {

			// valid annotation
			MqConsumer annotation = consumer.getClass().getAnnotation(MqConsumer.class);
			if (annotation == null) {
				throw new RuntimeException("xxl-mq, MqConsumer(" + consumer.getClass() + "), annotation is not exists.");
			}

			// valid group
			// ??????group???null
			if (annotation.group() == null || annotation.group().trim().length() == 0) {
				// empty group means consume broadcast message, will replace by uuid
				try {
					// annotation memberValues
					InvocationHandler invocationHandler = Proxy.getInvocationHandler(annotation);
					Field mValField = invocationHandler.getClass().getDeclaredField("memberValues");
					mValField.setAccessible(true);
					Map memberValues = (Map) mValField.get(invocationHandler);

					// set data for "group"
					String randomGroup = UUID.randomUUID().toString().replaceAll("-", "");
					memberValues.put("group", randomGroup);
				} catch (Exception e) {
					throw new RuntimeException("xxl-mq, MqConsumer(" + consumer.getClass() + "), group empty and genereta error.");
				}

			}
			if (annotation.group() == null || annotation.group().trim().length() == 0) {
				throw new RuntimeException("xxl-mq, MqConsumer(" + consumer.getClass() + "),group is empty.");
			}

			// valid topic
			if (annotation.topic() == null || annotation.topic().trim().length() == 0) {
				throw new RuntimeException("xxl-mq, MqConsumer(" + consumer.getClass() + "), topic is empty.");
			}

			// consumer map
			consumerRespository.add(new ConsumerThread(consumer));
		}
	}

	private void startConsumer() {

		// valid
		if (consumerRespository == null || consumerRespository.size() == 0) {
			return;
		}

		// registry consumer
		// ???consumer????????????????????????????????????????????????
		getConsumerRegistryHelper().registerConsumer(consumerRespository);

		// execute thread
		for (ConsumerThread item : consumerRespository) {
			clientFactoryThreadPool.execute(item);
			logger.info(">>>>>>>>>>> xxl-mq, consumer init success, , topic:{}, group:{}", item.getMqConsumer().topic(), item.getMqConsumer().group());
		}

	}

	private void destoryConsumer() {

		// valid
		if (consumerRespository == null || consumerRespository.size() == 0) {
			return;
		}

		// stop registry consumer
		getConsumerRegistryHelper().removeConsumer(consumerRespository);

	}


}
