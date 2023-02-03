package com.xxl.rpc.core.remoting.provider.impl;

import com.xxl.rpc.core.remoting.provider.XxlRpcProviderFactory;
import com.xxl.rpc.core.remoting.provider.annotation.XxlRpcService;
import com.xxl.rpc.core.util.XxlRpcException;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

import java.util.Map;

/**
 * xxl-rpc provider (for spring)
 *
 * @author xuxueli 2018-10-18 18:09:20
 */
public class XxlRpcSpringProviderFactory extends XxlRpcProviderFactory implements ApplicationContextAware, InitializingBean,DisposableBean {

    /**
     * 1、Spring启动的时候，就找到所有的添加了XxlRpcService注解的类
     * 2、然后将对应的接口名+version作为key，Bean作为map缓存起来
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        // 去spring 的ioc 容器中查找带有XxlRpcService注解的所有类
        Map<String, Object> serviceBeanMap = applicationContext.getBeansWithAnnotation(XxlRpcService.class);
        if (serviceBeanMap!=null && serviceBeanMap.size()>0) {
            for (Object serviceBean : serviceBeanMap.values()) {
                // valid
                //判断一下看看实现接口了没有
                if (serviceBean.getClass().getInterfaces().length ==0) {
                    throw new XxlRpcException("xxl-rpc, service(XxlRpcService) must inherit interface.");
                }
                // add service
                XxlRpcService xxlRpcService = serviceBean.getClass().getAnnotation(XxlRpcService.class);
                //找到类上有XxlRpcService注解，对应接口全类名
                String iface = serviceBean.getClass().getInterfaces()[0].getName();
                // 找到注解上面的版本信息
                String version = xxlRpcService.version();

                super.addService(iface, version, serviceBean);
            }
        }

        // TODO，addServices by api + prop

    }

    // 该方法的执行时机是 当设置好类属性的时候
    @Override
    public void afterPropertiesSet() throws Exception {
        super.start();
    }

    @Override
    public void destroy() throws Exception {
        super.stop();
    }

}
