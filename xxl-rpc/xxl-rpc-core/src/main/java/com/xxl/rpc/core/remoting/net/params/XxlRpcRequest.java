package com.xxl.rpc.core.remoting.net.params;

import java.io.Serializable;
import java.util.Arrays;

/**
 * request
 *
 * @author xuxueli 2015-10-29 19:39:12
 */
public class XxlRpcRequest implements Serializable{
	private static final long serialVersionUID = 42L;
	
	private String requestId;           // 唯一的标识，请求与响应对应，作者使用的uuid
	private long createMillisTime;      // 时间戳，主要是用于服务提供者进行超时检测用的
	private String accessToken;         // 用于服务调用者权限验证用的

    private String className;           // 需要调用的类的全类名
    private String methodName;          // 该类的方法名,服务调用者找到要执行的对象后就会invoke该方法
    private Class<?>[] parameterTypes;  // 方法参数类型数组
    private Object[] parameters;        // 方法参数的值

	private String version;             // 服务提供着版本，这个成员与 className 可以生成一个key，服务调用者根据该key就能够找到执行对象


	public String getRequestId() {
		return requestId;
	}

	public void setRequestId(String requestId) {
		this.requestId = requestId;
	}

	public long getCreateMillisTime() {
		return createMillisTime;
	}

	public void setCreateMillisTime(long createMillisTime) {
		this.createMillisTime = createMillisTime;
	}

	public String getAccessToken() {
		return accessToken;
	}

	public void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getMethodName() {
		return methodName;
	}

	public void setMethodName(String methodName) {
		this.methodName = methodName;
	}

	public Class<?>[] getParameterTypes() {
		return parameterTypes;
	}

	public void setParameterTypes(Class<?>[] parameterTypes) {
		this.parameterTypes = parameterTypes;
	}

	public Object[] getParameters() {
		return parameters;
	}

	public void setParameters(Object[] parameters) {
		this.parameters = parameters;
	}

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	@Override
	public String toString() {
		return "XxlRpcRequest{" +
				"requestId='" + requestId + '\'' +
				", createMillisTime=" + createMillisTime +
				", accessToken='" + accessToken + '\'' +
				", className='" + className + '\'' +
				", methodName='" + methodName + '\'' +
				", parameterTypes=" + Arrays.toString(parameterTypes) +
				", parameters=" + Arrays.toString(parameters) +
				", version='" + version + '\'' +
				'}';
	}

}
