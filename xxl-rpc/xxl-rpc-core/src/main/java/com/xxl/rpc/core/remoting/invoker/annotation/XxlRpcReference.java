package com.xxl.rpc.core.remoting.invoker.annotation;

import com.xxl.rpc.core.remoting.invoker.call.CallType;
import com.xxl.rpc.core.remoting.invoker.route.LoadBalance;
import com.xxl.rpc.core.remoting.net.Client;
import com.xxl.rpc.core.remoting.net.impl.netty.client.NettyClient;
import com.xxl.rpc.core.serialize.Serializer;
import com.xxl.rpc.core.serialize.impl.HessianSerializer;

import java.lang.annotation.*;

/**
 * rpc service annotation, skeleton of stub ("@Inherited" allow service use "Transactional")
 *
 * @author 2015-10-29 19:44:33
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface XxlRpcReference {

    //通讯方式 ，缺省netty
    Class<? extends Client> client() default NettyClient.class;

    //序列化方式 ，缺省hessian
    Class<? extends Serializer> serializer() default HessianSerializer.class;

    // 调用方式 ，缺省sync
    CallType callType() default CallType.SYNC;

    // 负载均衡策略，缺省round
    LoadBalance loadBalance() default LoadBalance.ROUND;

    //Class<?> iface;
    // version
    String version() default "";

    // 超时时间
    long timeout() default 1000;

    // 服务提供方地址， 这边可以自己来配置
    String address() default "";

    // token 做验证使用
    String accessToken() default "";

    //XxlRpcInvokeCallback invokeCallback() ;

}
