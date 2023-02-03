package com.xxl.job.core.executor.impl;

import com.xxl.job.core.executor.XxlJobExecutor;
import com.xxl.job.core.glue.GlueFactory;
import com.xxl.job.core.handler.annotation.XxlJob;
import com.xxl.job.core.handler.impl.MethodJobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.annotation.AnnotatedElementUtils;

import java.lang.reflect.Method;
import java.util.Map;


/**
 * xxl-job executor (for spring)
 *
 * @author xuxueli 2018-11-01 09:24:52
 */
public class XxlJobSpringExecutor extends XxlJobExecutor implements ApplicationContextAware, SmartInitializingSingleton, DisposableBean {
	private static final Logger logger = LoggerFactory.getLogger(XxlJobSpringExecutor.class);
	// ---------------------- applicationContext ----------------------
	private static ApplicationContext applicationContext;

	public static ApplicationContext getApplicationContext() {
		return applicationContext;
	}


    /*private void initJobHandlerRepository(ApplicationContext applicationContext) {
        if (applicationContext == null) {
            return;
        }

        // init job handler action
        Map<String, Object> serviceBeanMap = applicationContext.getBeansWithAnnotation(JobHandler.class);

        if (serviceBeanMap != null && serviceBeanMap.size() > 0) {
            for (Object serviceBean : serviceBeanMap.values()) {
                if (serviceBean instanceof IJobHandler) {
                    String name = serviceBean.getClass().getAnnotation(JobHandler.class).value();
                    IJobHandler handler = (IJobHandler) serviceBean;
                    if (loadJobHandler(name) != null) {
                        throw new RuntimeException("xxl-job jobhandler[" + name + "] naming conflicts.");
                    }
                    registJobHandler(name, handler);
                }
            }
        }
    }*/

	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		XxlJobSpringExecutor.applicationContext = applicationContext;
	}

	// start
	// 可以发现本类注入了 ApplicationContext 对象。以及实现了 SmartInitializingSingleton 接口，实现该接口的当spring容器初始完成，
	// 紧接着执行监听器发送监听后，就会遍历所有的Bean然后初始化所有单例非懒加载的bean，最后在实例化阶段结束时触发回调接口。
	@Override
	public void afterSingletonsInstantiated() {

		// init JobHandler Repository
		/*initJobHandlerRepository(applicationContext);*/

		// init JobHandler Repository (for method)
		// 初始化调度器资源管理器
		initJobHandlerMethodRepository(applicationContext);

		// refresh GlueFactory 刷新GlueFactory
		GlueFactory.refreshInstance(1);

		// super start
		try {
			super.start();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	// destroy
	@Override
	public void destroy() {
		super.destroy();
	}

	/**
	 * 该方法主要做了如下事情：
	 * <p>
	 * 1.从spring容器获取所有对象，并遍历查找方法上标记XxlJob注解的方法
	 * 2. 将xxljob配置的jobname作为key，对象,执行,初始,销毁的方法组成一个对象作为value，put到jobHandlerRepository中
	 */
	private void initJobHandlerMethodRepository(ApplicationContext applicationContext) {
		if (applicationContext == null) {
			return;
		}
		// init job handler from method
		String[] beanDefinitionNames = applicationContext.getBeanNamesForType(Object.class, false, true);
		for (String beanDefinitionName : beanDefinitionNames) {
			Object bean = applicationContext.getBean(beanDefinitionName);

			Map<Method, XxlJob> annotatedMethods = null;   // referred to ：org.springframework.context.event.EventListenerMethodProcessor.processBean
			try {
				annotatedMethods = MethodIntrospector.selectMethods(bean.getClass(),
						new MethodIntrospector.MetadataLookup<XxlJob>() {
							@Override
							public XxlJob inspect(Method method) {
								return AnnotatedElementUtils.findMergedAnnotation(method, XxlJob.class);
							}
						});
			} catch (Throwable ex) {
				logger.error("xxl-job method-jobhandler resolve error for bean[" + beanDefinitionName + "].", ex);
			}
			if (annotatedMethods == null || annotatedMethods.isEmpty()) {
				continue;
			}

			for (Map.Entry<Method, XxlJob> methodXxlJobEntry : annotatedMethods.entrySet()) {
				Method executeMethod = methodXxlJobEntry.getKey();
				XxlJob xxlJob = methodXxlJobEntry.getValue();
				// regist
				registJobHandler(xxlJob, bean, executeMethod);
			}
		}
	}

}
