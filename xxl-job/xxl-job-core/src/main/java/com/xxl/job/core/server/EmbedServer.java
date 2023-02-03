package com.xxl.job.core.server;

import com.xxl.job.core.biz.ExecutorBiz;
import com.xxl.job.core.biz.impl.ExecutorBizImpl;
import com.xxl.job.core.biz.model.*;
import com.xxl.job.core.thread.ExecutorRegistryThread;
import com.xxl.job.core.util.GsonTool;
import com.xxl.job.core.util.ThrowableUtil;
import com.xxl.job.core.util.XxlJobRemotingUtil;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.*;

/**
 * Copy from : https://github.com/xuxueli/xxl-rpc
 *
 * @author xuxueli 2020-04-11 21:25
 */
public class EmbedServer {
	private static final Logger logger = LoggerFactory.getLogger(EmbedServer.class);

	private ExecutorBiz executorBiz;
	private Thread thread;

	/**
	 * 干了几件事
	 * 1、启动了netty服务
	 * 2、开始服务注册，将服务注册到xxl-job
	 */
	public void start(final String address, final int port, final String appname, final String accessToken) {
		// 创建业务实例
		executorBiz = new ExecutorBizImpl();
		thread = new Thread(new Runnable() {
			@Override
			public void run() {
				// param
				// 用于接受ServerSocketChannel的io请求，再把请求具体执行的回调函数转交给worker group执行
				EventLoopGroup bossGroup = new NioEventLoopGroup();
				EventLoopGroup workerGroup = new NioEventLoopGroup();
				// 创建业务消费线程
				ThreadPoolExecutor bizThreadPool = new ThreadPoolExecutor(
						0,
						200,
						60L,
						TimeUnit.SECONDS,
						new LinkedBlockingQueue<Runnable>(2000),
						new ThreadFactory() {
							@Override
							public Thread newThread(Runnable r) {
								return new Thread(r, "xxl-job, EmbedServer bizThreadPool-" + r.hashCode());
							}
						},
						new RejectedExecutionHandler() {
							@Override
							public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
								throw new RuntimeException("xxl-job, EmbedServer bizThreadPool is EXHAUSTED!");
							}
						});
				try {
					// start server
					// 启动netty服务
					ServerBootstrap bootstrap = new ServerBootstrap();
					bootstrap.group(bossGroup, workerGroup)
							.channel(NioServerSocketChannel.class)
							.childHandler(new ChannelInitializer<SocketChannel>() {
								@Override
								public void initChannel(SocketChannel channel) throws Exception {
									channel.pipeline()
											// 空闲检测
											.addLast(new IdleStateHandler(0, 0, 30 * 3, TimeUnit.SECONDS))  // beat 3N, close if idle
											// http编码处理类
											.addLast(new HttpServerCodec())
											// post请求参数解析器
											.addLast(new HttpObjectAggregator(5 * 1024 * 1024))  // merge request & reponse to FULL
											// 自定义业务handler
											.addLast(new EmbedHttpServerHandler(executorBiz, accessToken, bizThreadPool));
								}
							})
							.childOption(ChannelOption.SO_KEEPALIVE, true);

					// bind
					// 将端口号绑定到netty服务上
					ChannelFuture future = bootstrap.bind(port).sync();

					logger.info(">>>>>>>>>>> xxl-job remoting server start success, nettype = {}, port = {}", EmbedServer.class, port);

					// start registry
					// 开始服务注册，将服务注册到xxl-job，就是发送注册和心跳请求（xxl-job中是一个请求）
					startRegistry(appname, address);

					// wait util stop
					// 同步阻塞线程
					future.channel().closeFuture().sync();

				} catch (InterruptedException e) {
					logger.info(">>>>>>>>>>> xxl-job remoting server stop.");
				} catch (Exception e) {
					logger.error(">>>>>>>>>>> xxl-job remoting server error.", e);
				} finally {
					// stop
					try {
						// 执行到此处表示netty服务停止，释放资源
						workerGroup.shutdownGracefully();
						bossGroup.shutdownGracefully();
					} catch (Exception e) {
						logger.error(e.getMessage(), e);
					}
				}
			}
		});
		// 设置守护线程，防止netty线程被回收
		thread.setDaemon(true);    // daemon, service jvm, user thread leave >>> daemon leave >>> jvm leave
		// 启动线程
		thread.start();
	}

	public void stop() throws Exception {
		// destroy server thread
		if (thread != null && thread.isAlive()) {
			thread.interrupt();
		}

		// stop registry
		stopRegistry();
		logger.info(">>>>>>>>>>> xxl-job remoting server destroy success.");
	}


	// ---------------------- registry ----------------------

	public void startRegistry(final String appname, final String address) {
		// start registry
		ExecutorRegistryThread.getInstance().start(appname, address);
	}

	// ---------------------- registry ----------------------

	public void stopRegistry() {
		// stop registry
		ExecutorRegistryThread.getInstance().toStop();
	}

	/**
	 * netty_http
	 * <p>
	 * Copy from : https://github.com/xuxueli/xxl-rpc
	 *
	 * @author xuxueli 2015-11-24 22:25:15
	 */
	public static class EmbedHttpServerHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
		private static final Logger logger = LoggerFactory.getLogger(EmbedHttpServerHandler.class);

		private ExecutorBiz executorBiz;
		private String accessToken;
		private ThreadPoolExecutor bizThreadPool;

		public EmbedHttpServerHandler(ExecutorBiz executorBiz, String accessToken, ThreadPoolExecutor bizThreadPool) {
			this.executorBiz = executorBiz;
			this.accessToken = accessToken;
			this.bizThreadPool = bizThreadPool;
		}

		@Override
		protected void channelRead0(final ChannelHandlerContext ctx, FullHttpRequest msg) throws Exception {
			// request parse
			//final byte[] requestBytes = ByteBufUtil.getBytes(msg.content());    // byteBuf.toString(io.netty.util.CharsetUtil.UTF_8);
			// 消息解码
			String requestData = msg.content().toString(CharsetUtil.UTF_8);
			// 请求uri，获取uri,后面通过uri来处理不同的请求
			String uri = msg.uri();
			// 获取请求方式,Post/Get
			HttpMethod httpMethod = msg.method();
			// 保持长连接
			boolean keepAlive = HttpUtil.isKeepAlive(msg);
			String accessTokenReq = msg.headers().get(XxlJobRemotingUtil.XXL_JOB_ACCESS_TOKEN);
			// 业务线程执行处理
			// invoke
			bizThreadPool.execute(new Runnable() {
				@Override
				public void run() {
					// do invoke
					Object responseObj = process(httpMethod, uri, requestData, accessTokenReq);
					// to json
					// 返回对象转换json
					String responseJson = GsonTool.toJson(responseObj);

					// write response
					// 返回
					writeResponse(ctx, keepAlive, responseJson);
				}
			});
		}

		private Object process(HttpMethod httpMethod, String uri, String requestData, String accessTokenReq) {
			// valid
			// 校验请求方式，token，uri
			if (HttpMethod.POST != httpMethod) {
				return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, HttpMethod not support.");
			}
			if (uri == null || uri.trim().length() == 0) {
				return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping empty.");
			}
			if (accessToken != null
					&& accessToken.trim().length() > 0
					&& !accessToken.equals(accessTokenReq)) {
				return new ReturnT<String>(ReturnT.FAIL_CODE, "The access token is wrong.");
			}

			// services mapping
			// 处理业务
			try {
				switch (uri) {
					case "/beat":
						// 心跳检查
						return executorBiz.beat();
					case "/idleBeat":
						// 任务空闲检查
						IdleBeatParam idleBeatParam = GsonTool.fromJson(requestData, IdleBeatParam.class);
						return executorBiz.idleBeat(idleBeatParam);
					case "/run":
						// 运行一个任务
						TriggerParam triggerParam = GsonTool.fromJson(requestData, TriggerParam.class);
						return executorBiz.run(triggerParam);
					case "/kill":
						// 杀死一个任务
						KillParam killParam = GsonTool.fromJson(requestData, KillParam.class);
						return executorBiz.kill(killParam);
					case "/log":
						// 获得客户端记录的日志
						LogParam logParam = GsonTool.fromJson(requestData, LogParam.class);
						return executorBiz.log(logParam);
					default:
						// 无法解析的业务uri，异常返回
						return new ReturnT<String>(ReturnT.FAIL_CODE, "invalid request, uri-mapping(" + uri + ") not found.");
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
				return new ReturnT<String>(ReturnT.FAIL_CODE, "request error:" + ThrowableUtil.toString(e));
			}
		}

		/**
		 * write response
		 */
		private void writeResponse(ChannelHandlerContext ctx, boolean keepAlive, String responseJson) {
			// write response
			FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.copiedBuffer(responseJson, CharsetUtil.UTF_8));   //  Unpooled.wrappedBuffer(responseJson)
			response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/html;charset=UTF-8");       // HttpHeaderValues.TEXT_PLAIN.toString()
			response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
			if (keepAlive) {
				response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
			}
			ctx.writeAndFlush(response);
		}

		@Override
		public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
			ctx.flush();
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
			logger.error(">>>>>>>>>>> xxl-job provider netty_http server caught exception", cause);
			ctx.close();
		}

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
			if (evt instanceof IdleStateEvent) {
				ctx.channel().close();      // beat 3N, close if idle
				logger.debug(">>>>>>>>>>> xxl-job provider netty_http server close an idle channel.");
			} else {
				super.userEventTriggered(ctx, evt);
			}
		}
	}
}