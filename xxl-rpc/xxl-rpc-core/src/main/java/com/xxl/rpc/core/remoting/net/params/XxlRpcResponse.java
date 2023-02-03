package com.xxl.rpc.core.remoting.net.params;

import java.io.Serializable;

/**
 * response
 * 其实通过这两个请求响应实体的字段就可以看出rpc的很重要实现就是反射。通过反射请求的类与方法获得结果，然后将结果响应给服务调用方。
 *
 * @author xuxueli 2015-10-29 19:39:54
 */
public class XxlRpcResponse implements Serializable{
	private static final long serialVersionUID = 42L;


	private String requestId;  // 与XxlRpcRequest中的requestId一一对应，为的就是异步的时候能找到对应的响应。
    private String errorMsg;   // 错误信息，超时，权限验证失败
    private Object result;     // 方法的执行结果。


    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    public Object getResult() {
        return result;
    }

    public void setResult(Object result) {
        this.result = result;
    }

    @Override
    public String toString() {
        return "XxlRpcResponse{" +
                "requestId='" + requestId + '\'' +
                ", errorMsg='" + errorMsg + '\'' +
                ", result=" + result +
                '}';
    }

}
