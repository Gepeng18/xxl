package com.xxl.rpc.core.remoting.invoker.impl;

import com.xxl.rpc.core.registry.Register;
import com.xxl.rpc.core.remoting.invoker.XxlRpcInvokerFactory;
import com.xxl.rpc.core.remoting.invoker.annotation.XxlRpcReference;
import com.xxl.rpc.core.remoting.invoker.reference.XxlRpcReferenceBean;
import com.xxl.rpc.core.remoting.provider.XxlRpcProviderFactory;
import com.xxl.rpc.core.util.XxlRpcException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.config.InstantiationAwareBeanPostProcessorAdapter;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * xxl-rpc invoker factory, init service-registry and spring-bean by annotation (for spring)
 * BeanFactoryAware ： 注入 beanfactory 使用。可以使用beanfactory获得 spring中bean。他这边就一个setBeanFactory 方法。
 * InitializingBean ： 在bean实例化设置完属性后调用 afterPropertiesSet方法
 * InstantiationAwareBeanPostProcessorAdapter： 对实例化之后的bean进行增强。调用postProcessAfterInstantiation 方法。
 * DisposableBean：bean 销毁的时候调用destroy方法。
 * 执行顺序
 * setBeanFactory() ----> afterPropertiesSet() ---->postProcessAfterInstantiation() ---->destroy()。
 *
 * @author xuxueli 2018-10-19
 */
public class XxlRpcSpringInvokerFactory extends InstantiationAwareBeanPostProcessorAdapter implements InitializingBean,DisposableBean, BeanFactoryAware {
    private Logger logger = LoggerFactory.getLogger(XxlRpcSpringInvokerFactory.class);

    // ---------------------- config ----------------------

    private Class<? extends Register> serviceRegistryClass;          // class.forname
    private Map<String, String> serviceRegistryParam;


    public void setServiceRegistryClass(Class<? extends Register> serviceRegistryClass) {
        this.serviceRegistryClass = serviceRegistryClass;
    }

    public void setServiceRegistryParam(Map<String, String> serviceRegistryParam) {
        this.serviceRegistryParam = serviceRegistryParam;
    }


    // ---------------------- util ----------------------

    private XxlRpcInvokerFactory xxlRpcInvokerFactory;

    @Override
    public void afterPropertiesSet() throws Exception {
        // start invoker factory
        xxlRpcInvokerFactory = new XxlRpcInvokerFactory(serviceRegistryClass, serviceRegistryParam);
        xxlRpcInvokerFactory.start();
    }

    /**
     * 该方法 主要是找出带有XxlRpcReference注解的成员变量。然后解析XxlRpcReference注解中的参数，创建XxlRpcReferenceBean对象，
     * 生成代理对象，然后将代理对象赋值给这个字段，这样，在我们使用这个service的时候，真正调用的就是这个代理对象。
     * 接下来进行服务发现， 将所有本地serviceKeylist中的key进行registry，但是不接受返回结果。（综下，这一步的目的就是为了确保本地所有seviceKey都有对应的服务）
     */
    @Override
    public boolean postProcessAfterInstantiation(final Object bean, final String beanName) throws BeansException {

        // collection
        final Set<String> serviceKeyList = new HashSet<>();

        // parse XxlRpcReferenceBean
        ReflectionUtils.doWithFields(bean.getClass(), new ReflectionUtils.FieldCallback() {
            @Override
            public void doWith(Field field) throws IllegalArgumentException, IllegalAccessException {
                // 字段 是否有XxlRpcReference 注解
                if (field.isAnnotationPresent(XxlRpcReference.class)) {
                    // valid 获取字段的类型
                    Class iface = field.getType();
                    // 字段不是接口抛出异常
                    if (!iface.isInterface()) {
                        throw new XxlRpcException("xxl-rpc, reference(XxlRpcReference) must be interface.");
                    }

                    // 获取注解信息，
                    XxlRpcReference rpcReference = field.getAnnotation(XxlRpcReference.class);

                    // init reference bean   创建 referenceBean ， 设置一些需要的参数 ，将注解中的参数 设置到referenceBean 对象中。
                    XxlRpcReferenceBean referenceBean = new XxlRpcReferenceBean();
                    referenceBean.setClient(rpcReference.client());
                    referenceBean.setSerializer(rpcReference.serializer());
                    referenceBean.setCallType(rpcReference.callType());
                    referenceBean.setLoadBalance(rpcReference.loadBalance());
                    referenceBean.setIface(iface);
                    referenceBean.setVersion(rpcReference.version());
                    referenceBean.setTimeout(rpcReference.timeout());
                    referenceBean.setAddress(rpcReference.address());
                    referenceBean.setAccessToken(rpcReference.accessToken());
                    referenceBean.setInvokeCallback(null);
                    referenceBean.setInvokerFactory(xxlRpcInvokerFactory);


                    // get proxyObj
                    Object serviceProxy = null;
                    try {
                        // 调用 getObject方法， 这个方法主要使用生成代理对象的。
                        serviceProxy = referenceBean.getObject();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }

                    // set bean
                    field.setAccessible(true);
                    //给这个字段赋值
                    field.set(bean, serviceProxy);

                    logger.info(">>>>>>>>>>> xxl-rpc, invoker factory init reference bean success. serviceKey = {}, bean.field = {}.{}",
                            XxlRpcProviderFactory.makeServiceKey(iface.getName(), rpcReference.version()), beanName, field.getName());

                    // collection
                    // 根据这个接口全类名与 version生成 servicekey  ，跟服务提供者一样的
                    String serviceKey = XxlRpcProviderFactory.makeServiceKey(iface.getName(), rpcReference.version());
                    serviceKeyList.add(serviceKey);

                }
            }
        });

        // mult discovery  进行服务发现， 本地缓存一下 这个key的服务提供者
        if (xxlRpcInvokerFactory.getRegister() != null) {
            try {
                xxlRpcInvokerFactory.getRegister().discovery(serviceKeyList);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }

        return super.postProcessAfterInstantiation(bean, beanName);
    }


    @Override
    public void destroy() throws Exception {

        // stop invoker factory
        xxlRpcInvokerFactory.stop();
    }

    private BeanFactory beanFactory;

    @Override
    public void setBeanFactory(BeanFactory beanFactory) throws BeansException {
        this.beanFactory = beanFactory;
    }
}
