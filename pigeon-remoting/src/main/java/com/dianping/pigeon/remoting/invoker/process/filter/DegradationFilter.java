/**
 * Dianping.com Inc.
 * Copyright (c) 2003-2013 All Rights Reserved.
 */
package com.dianping.pigeon.remoting.invoker.process.filter;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.dianping.pigeon.remoting.common.exception.ApplicationException;
import com.dianping.pigeon.remoting.invoker.concurrent.*;
import com.dianping.pigeon.remoting.invoker.exception.*;
import org.apache.commons.lang.StringUtils;

import com.dianping.pigeon.config.ConfigChangeListener;
import com.dianping.pigeon.config.ConfigManager;
import com.dianping.pigeon.config.ConfigManagerLoader;
import com.dianping.pigeon.log.Logger;
import com.dianping.pigeon.log.LoggerLoader;
import com.dianping.pigeon.monitor.*;
import com.dianping.pigeon.remoting.common.monitor.trace.InvokerMonitorData;
import com.dianping.pigeon.remoting.common.codec.json.JacksonSerializer;
import com.dianping.pigeon.remoting.common.domain.CallMethod;
import com.dianping.pigeon.remoting.common.domain.InvocationContext.TimePhase;
import com.dianping.pigeon.remoting.common.domain.InvocationContext.TimePoint;
import com.dianping.pigeon.remoting.common.domain.InvocationResponse;
import com.dianping.pigeon.remoting.common.exception.RejectedException;
import com.dianping.pigeon.remoting.common.exception.RpcException;
import com.dianping.pigeon.remoting.common.process.ServiceInvocationHandler;
import com.dianping.pigeon.remoting.common.util.Constants;
import com.dianping.pigeon.remoting.common.util.GroovyUtils;
import com.dianping.pigeon.remoting.invoker.config.InvokerConfig;
import com.dianping.pigeon.remoting.invoker.domain.DefaultInvokerContext;
import com.dianping.pigeon.remoting.invoker.domain.InvokerContext;
import com.dianping.pigeon.remoting.invoker.process.DegradationManager;
import com.dianping.pigeon.remoting.invoker.process.DegradationManager.DegradeActionConfig;
import com.dianping.pigeon.remoting.invoker.process.ExceptionManager;
import com.dianping.pigeon.remoting.invoker.proxy.GroovyScriptInvocationProxy;
import com.dianping.pigeon.remoting.invoker.proxy.MockInvocationUtils;
import com.dianping.pigeon.remoting.invoker.proxy.MockProxyWrapper;
import com.dianping.pigeon.remoting.invoker.route.quality.RequestQualityManager;
import com.dianping.pigeon.remoting.invoker.util.InvokerHelper;
import com.dianping.pigeon.remoting.invoker.util.InvokerUtils;
import groovy.lang.Script;

/**
 * @author xiangwu
 */
public class DegradationFilter extends InvocationInvokeFilter {

    private static final Logger logger = LoggerLoader.getLogger(DegradationFilter.class);
    private static final ConfigManager configManager = ConfigManagerLoader.getConfigManager();
    private static final String KEY_DEGRADE_METHODS = "pigeon.invoker.degrade.methods";
    private static final String KEY_DEGRADE_METHOD = "pigeon.invoker.degrade.method.return.";
    private static final InvocationResponse NO_RETURN_RESPONSE = InvokerUtils.createNoReturnResponse();
    private static volatile Map<String, DegradeAction> degradeMethodActions = new ConcurrentHashMap<String, DegradeAction>();
    private static final JacksonSerializer jacksonSerializer = new JacksonSerializer();
    // service#method --> groovyMockProxy
    private final static ConcurrentHashMap<String, Object> groovyMocks = new ConcurrentHashMap<>();

    static {
        String degradeMethodsConfig = configManager.getStringValue(KEY_DEGRADE_METHODS);
        try {
            parseDegradeMethodsConfig(degradeMethodsConfig);
        } catch (Throwable t) {
            logger.error("Error while parsing degradation configuration:" + degradeMethodsConfig, t);
            throw new IllegalArgumentException("Error while parsing degradation configuration:" + degradeMethodsConfig,
                    t);
        }
        configManager.registerConfigChangeListener(new InnerConfigChangeListener());
    }

    private static class InnerConfigChangeListener implements ConfigChangeListener {

        @Override
        public void onKeyUpdated(String key, String value) {
            if (key.endsWith(KEY_DEGRADE_METHODS)) {
                try {
                    parseDegradeMethodsConfig(value);
                } catch (Throwable t) {
                    logger.error("Error while parsing degradation configuration:" + value, t);
                }
            }
        }

        @Override
        public void onKeyAdded(String key, String value) {

        }

        @Override
        public void onKeyRemoved(String key) {

        }

    }

    private static void parseDegradeMethodsConfig(String degradeMethodsConfig) throws Throwable {
        if (StringUtils.isNotBlank(degradeMethodsConfig)) {
            ConcurrentHashMap<String, DegradeAction> map = new ConcurrentHashMap<String, DegradeAction>();
            // 格式如"key1=value1,key2=value2"，其中key为{url}#{methodName}
            // 可以从配置"pigeon.invoker.degrade.method.return.{valueN}"中获取具体方法的DegradeActionConfig数据
            String[] pairArray = degradeMethodsConfig.split(",");
            for (String str : pairArray) {
                if (StringUtils.isNotBlank(str)) {
                    String[] pair = str.split("=");
                    if (pair != null && pair.length == 2) {
                        String key = pair[1].trim();
                        DegradeAction degradeAction = new DegradeAction();
                        if (StringUtils.isNotBlank(key)) {
                            // 获取服务方法对应的DegradeActionConfig，用来初始化DegradeAction对象
                            String config = configManager.getStringValue(KEY_DEGRADE_METHOD + key);

                            if (StringUtils.isNotBlank(config)) {
                                // 反序列化为degradeActionConfig实例
                                config = config.trim();
                                config = "{\"@class\":\"" + DegradeActionConfig.class.getName() + "\","
                                        + config.substring(1);

                                DegradeActionConfig degradeActionConfig = (DegradeActionConfig) jacksonSerializer
                                        .toObject(DegradeActionConfig.class, config);

                                // 初始化degradeAction对象
                                degradeAction.setUseMockClass(degradeActionConfig.getUseMockClass());
                                degradeAction.setUseGroovyScript(degradeActionConfig.getUseGroovyScript());
                                degradeAction.setThrowException(degradeActionConfig.getThrowException());
                                degradeAction.setEnable(degradeActionConfig.getEnable());
                                String content = degradeActionConfig.getContent();
                                Object returnObj = null;

                                // 解析具体的降级方案
                                if (degradeAction.isUseMockClass()) {
                                    // use mock class
                                } else if (degradeAction.isUseGroovyScript()) {
                                    degradeAction.setGroovyScript(GroovyUtils.getScript(content));
                                } else if (degradeAction.isThrowException()) {
                                    if (StringUtils.isNotBlank(degradeActionConfig.getReturnClass())) {
                                        // 反序列化为指定异常实例
                                        returnObj = jacksonSerializer
                                                .toObject(Class.forName(degradeActionConfig.getReturnClass()), content);
                                        if (!(returnObj instanceof Exception)) {
                                            throw new IllegalArgumentException(
                                                    "Invalid exception class:" + degradeActionConfig.getReturnClass());
                                        }
                                        degradeAction.setReturnObj(returnObj);
                                    }
                                } else {
                                    if (StringUtils.isNotBlank(degradeActionConfig.getKeyClass())
                                            && StringUtils.isNotBlank(degradeActionConfig.getValueClass())) {
                                        // 反序列化为Map对象
                                        returnObj = jacksonSerializer.deserializeMap(content,
                                                Class.forName(degradeActionConfig.getReturnClass()),
                                                Class.forName(degradeActionConfig.getKeyClass()),
                                                Class.forName(degradeActionConfig.getValueClass()));
                                    } else if (StringUtils.isNotBlank(degradeActionConfig.getComponentClass())) {
                                        // 反序列化为Collection对象
                                        returnObj = jacksonSerializer.deserializeCollection(content,
                                                Class.forName(degradeActionConfig.getReturnClass()),
                                                Class.forName(degradeActionConfig.getComponentClass()));
                                    } else if (StringUtils.isNotBlank(degradeActionConfig.getReturnClass())) {
                                        // 反序列为普通java对象
                                        returnObj = jacksonSerializer
                                                .toObject(Class.forName(degradeActionConfig.getReturnClass()), content);
                                    }
                                    degradeAction.setReturnObj(returnObj);
                                }
                            }
                        }
                        map.put(pair[0].trim(), degradeAction);
                    }
                }
            }
            degradeMethodActions.clear();
            degradeMethodActions = map; // 更新缓存
        } else {
            degradeMethodActions.clear();
        }

        groovyMocks.clear();
    }

    @Override
    public InvocationResponse invoke(ServiceInvocationHandler handler, InvokerContext context) throws Throwable {
        context.getTimeline().add(new TimePoint(TimePhase.D));

        InvocationResponse degradeResponse;
        if (DegradationManager.INSTANCE.needDegrade(context)) {
            degradeResponse = degradeCall(context);

            if (degradeResponse != null) { // 返回自动降级熔断的降级结果
                return degradeResponse;
            }
        }

        boolean failed = false;
        Throwable failedException = null;

        try {
            InvocationResponse response = handler.handle(context);
            Object responseReturn = response.getReturn();
            if (responseReturn != null) {
                int messageType = response.getMessageType();

                if (messageType == Constants.MESSAGE_TYPE_EXCEPTION) {
                    RpcException rpcException = InvokerUtils.toRpcException(response);
                    if (rpcException instanceof RemoteInvocationException
                            || rpcException instanceof RejectedException) {
                        failed = true;
                        failedException = rpcException;
                        if (DegradationManager.INSTANCE.needFailureDegrade(context)) {
                            // 对于自动降级、失败降级，请求服务端失败则置为true
                            context.getDegradeInfo().setFailureDegrade(true);
                            context.getDegradeInfo().setCause(rpcException);
                            degradeResponse = degradeCall(context);

                            if (degradeResponse != null) { // 返回同步调用模式的失败降级结果
                                return degradeResponse;
                            }
                        }
                    }
                } else if (messageType == Constants.MESSAGE_TYPE_SERVICE_EXCEPTION) {
                    // 如果捕捉到用户指定的业务异常,包装为降级异常捕捉
                    Exception exception = InvokerUtils.toApplicationException(response);
                    if (DegradationManager.INSTANCE.needFailureDegrade(context)
                            && DegradationManager.INSTANCE.isCustomizedDegradeException(exception)) {
                        failed = true;
                        failedException = exception;
                        if (DegradationManager.INSTANCE.needFailureDegrade(context)) {
                            context.getDegradeInfo().setFailureDegrade(true);
                            context.getDegradeInfo().setCause(exception);
                            degradeResponse = degradeCall(context);

                            if (degradeResponse != null) { // 返回同步调用模式的失败降级结果
                                return degradeResponse;
                            }
                        }
                    }
                }
            }

            InvokerConfig<?> invokerConfig = context.getInvokerConfig();
            byte callMethodCode = invokerConfig.getCallMethod(context.getMethodName());
            CallMethod callMethod = CallMethod.getCallMethod(callMethodCode);

            if (CallMethod.SYNC == callMethod) {
                if (failed) {
                    DegradationManager.INSTANCE.addFailedRequest(context, failedException);
                } else {
                    // 自动降级放行的请求、失败降级，请求成功时：here
                    DegradationManager.INSTANCE.addNormalRequest(context);
                }
            }

            return response;

        } catch (ServiceUnavailableException | RemoteInvocationException | RequestTimeoutException
                | RejectedException e) {
            failed = true;
            if (DegradationManager.INSTANCE.needFailureDegrade(context)) {
                context.getDegradeInfo().setFailureDegrade(true);
                context.getDegradeInfo().setCause(e);
                degradeResponse = degradeCall(context);

                if (degradeResponse != null) {// 返回同步调用模式的失败降级结果
                    return degradeResponse;
                }
            }

            // 自动降级放行的请求、失败降级，请求失败时：here
            DegradationManager.INSTANCE.addFailedRequest(context, e);
            throw e;
        } finally {
            RequestQualityManager.INSTANCE.addClientRequest(context, failed);
        }
    }

    public static InvocationResponse degradeCall(InvokerContext context) {
        InvocationResponse resp = doDegradeCall(context);
        if (resp != null) {

            InvokerMonitorData monitorData = (InvokerMonitorData) context.getMonitorData();

            if (monitorData != null) {
                monitorData.degrade();
            }

            // 自动降级放行的请求、失败降级，请求失败时：here
            if (context.getDegradeInfo().isFailureDegrade()) {
                DegradationManager.INSTANCE.addFailedRequest(context, new ServiceFailureDegreadedException());
            } else {
                // 强制降级、自动降级拦截的请求：here
                DegradationManager.INSTANCE.addDegradedRequest(context, null);
            }

        }
        return resp;
    }

    private static InvocationResponse doDegradeCall(InvokerContext context) {
        InvokerConfig<?> invokerConfig = context.getInvokerConfig();
        byte callMethodCode = invokerConfig.getCallMethod(context.getMethodName());
        CallMethod callMethod = CallMethod.getCallMethod(callMethodCode);

        InvocationResponse response = null;

        int timeout = invokerConfig.getTimeout(context.getMethodName());
        Integer timeoutThreadLocal = InvokerHelper.getTimeout();
        if (timeoutThreadLocal != null) {
            timeout = timeoutThreadLocal;
        }

        InvokerMonitorData monitorData = (InvokerMonitorData) context.getMonitorData();

        if (monitorData != null) {
            monitorData.setCallMethod(invokerConfig.getCallMethod());
            monitorData.setSerialize(invokerConfig.getSerialize());
            monitorData.setTimeout(timeout);
            monitorData.add();
        }

        // 线程内设置的默认结果，类似缓存
        Object defaultResult = InvokerHelper.getDefaultResult();
        // 获取服务方法配置的降级方案
        String key = DegradationManager.INSTANCE.getRequestUrl(context);
        DegradeAction action = degradeMethodActions.get(key);

        if (callMethod == CallMethod.FUTURE && context.getDegradeInfo().isFailureDegrade()) {
            callMethod = CallMethod.SYNC;
        }

        switch (callMethod) {
            case SYNC:
                try {
                    // 线程内存在默认结果，返回它
                    if (defaultResult != null) {
                        response = InvokerUtils.createDefaultResponse(defaultResult);
                    } else if (action != null) {
                        if (action.isUseMockClass()) {
                            Object mockObj = context.getInvokerConfig().getMock();
                            if (mockObj != null) {
                                // 反射调用配置的mock对象的方法，将方法执行结果作为降级响应
                                defaultResult = new MockProxyWrapper(mockObj).invoke(context.getMethodName(),
                                        context.getParameterTypes(), context.getArguments()); // 用Wrapper隐藏了反射调用的异常处理
                                response = InvokerUtils.createDefaultResponse(defaultResult);
                            }
                        } else if (action.isUseGroovyScript()) {
                            // 传入groovy脚本会生成相应的Script对象，此处通过代理对Script对象进行包装，反射调用代理方法从而执行脚本，将脚本执行结果作为降级响应
                            defaultResult = new MockProxyWrapper(getGroovyMockProxy(key, context, action))
                                    .invoke(context.getMethodName(), context.getParameterTypes(), context.getArguments());
                            response = InvokerUtils.createDefaultResponse(defaultResult);
                        } else if (action.isThrowException()) {
                            Exception exception;
                            // 如果配置了异常类型，则返回指定异常的实例，否则返回ServiceDegradedException的实例
                            if (action.getReturnObj() == null) {
                                exception = new ServiceDegradedException(key);
                            } else {
                                exception = (Exception) action.getReturnObj();
                            }
                            throw exception;
                        } else {
                            // 降级配置中的默认结果作为降级响应
                            defaultResult = action.getReturnObj();
                            response = InvokerUtils.createDefaultResponse(defaultResult);
                        }
                    }
                } catch (Throwable t) {
                    response = InvokerUtils.createDefaultResponse(t);
                    response.setMessageType(Constants.MESSAGE_TYPE_SERVICE_EXCEPTION);
                } finally {
                    if (response != null) {
                        // 标志为是降级响应，不是服务端的实际响应
                        context.getDegradeInfo().setDegrade(true);
                        addCurrentTimeData(timeout);
                    }
                }
                break;
            case CALLBACK:
                // 实现原理类似与SYNC，只是用callBack进行了一层封装
                try {
                    if (defaultResult != null) {
                        response = callBackOnSuccess(context, defaultResult);
                    } else if (action != null) {
                        if (action.isUseMockClass()) {
                            Object mockObj = context.getInvokerConfig().getMock();
                            if (mockObj != null) {
                                defaultResult = new MockProxyWrapper(mockObj).invoke(context.getMethodName(),
                                        context.getParameterTypes(), context.getArguments());
                                response = callBackOnSuccess(context, defaultResult);
                            }
                        } else if (action.isUseGroovyScript()) {
                            defaultResult = new MockProxyWrapper(getGroovyMockProxy(key, context, action))
                                    .invoke(context.getMethodName(), context.getParameterTypes(), context.getArguments());
                            response = callBackOnSuccess(context, defaultResult);
                        } else if (action.isThrowException()) {
                            Exception exception;
                            if (action.getReturnObj() == null) {
                                exception = new ServiceDegradedException(key);
                            } else {
                                exception = (Exception) action.getReturnObj();
                            }

                            throw exception;

                        } else {
                            defaultResult = action.getReturnObj();
                            response = callBackOnSuccess(context, defaultResult);
                        }
                    }
                } catch (Throwable t) {
                    if (t instanceof Exception) {
                        response = callBackOnfailure(context, (Exception) t);
                    } else {
                        response = callBackOnfailure(context, new ApplicationException(t));
                    }
                } finally {
                    if (response != null) {
                        context.getDegradeInfo().setDegrade(true);
                        addCurrentTimeData(timeout);
                        MonitorTransaction transaction = MonitorLoader.getMonitor().getCurrentCallTransaction();
                        if (transaction != null) {
                            DegradationManager.INSTANCE.monitorDegrade(context, transaction);
                        }
                    }
                }
                break;
            case FUTURE:
                // 实现原理类似与SYNC，只是用future进行了一层封装
                if (defaultResult != null) {
                    DegradeServiceFuture future = new DegradeServiceFuture(context, timeout);
                    FutureFactory.setFuture(future);
                    response = InvokerUtils.createFutureResponse(future);
                    future.callback(InvokerUtils.createDefaultResponse(defaultResult));
                    future.run();
                } else if (action != null) {
                    if (action.isUseMockClass()) {
                        Object mockObj = context.getInvokerConfig().getMock();
                        if (mockObj != null) {
                            MockProxyWrapper mockProxyWrapper = new MockProxyWrapper(mockObj);
                            MockCallbackFuture future = new MockCallbackFuture(mockProxyWrapper, context, timeout);
                            FutureFactory.setFuture(future);
                            response = InvokerUtils.createFutureResponse(future);
                            future.callback(response);
                            future.run();
                        }
                    } else if (action.isUseGroovyScript()) {
                        MockProxyWrapper mockProxyWrapper = new MockProxyWrapper(getGroovyMockProxy(key, context, action));
                        MockCallbackFuture future = new MockCallbackFuture(mockProxyWrapper, context, timeout);
                        FutureFactory.setFuture(future);
                        response = InvokerUtils.createFutureResponse(future);
                        future.callback(response);
                        future.run();
                    } else if (action.isThrowException()) {
                        Exception exception;
                        if (action.getReturnObj() == null) {
                            exception = new ServiceDegradedException(key);
                        } else {
                            exception = (Exception) action.getReturnObj();
                        }
                        DegradeServiceFuture future = new DegradeServiceFuture(context, timeout);
                        FutureFactory.setFuture(future);
                        response = InvokerUtils.createFutureResponse(future);
                        future.callback(InvokerUtils.createDefaultResponse(exception));
                        future.run();
                    } else {
                        defaultResult = action.getReturnObj();
                        DegradeServiceFuture future = new DegradeServiceFuture(context, timeout);
                        FutureFactory.setFuture(future);
                        response = InvokerUtils.createFutureResponse(future);
                        future.callback(InvokerUtils.createDefaultResponse(defaultResult));
                        future.run();
                    }
                }
                if (response != null) {
                    context.getDegradeInfo().setDegrade(true);
                    addCurrentTimeData(timeout);
                }
                break;
            case ONEWAY:
                // "不关心结果"的调用方式，返回"无结果"响应
                context.getDegradeInfo().setDegrade(true);
                addCurrentTimeData(timeout);
                response = NO_RETURN_RESPONSE;
                break;
        }

        if (response != null) {
            ((DefaultInvokerContext) context).setResponse(response);
        }

        return response;
    }

    private static void addCurrentTimeData(long timeout) {
        MonitorTransaction transaction = MonitorLoader.getMonitor().getCurrentCallTransaction();
        if (transaction != null) {
            transaction.addData("CurrentTimeout", timeout);
        }
    }

    private static InvocationResponse callBackOnSuccess(InvokerContext context, Object defaultResult) {
        InvocationCallback callback = context.getInvokerConfig().getCallback();
        InvocationCallback tlCallback = InvokerHelper.getCallback();
        if (tlCallback != null) {
            callback = tlCallback;
            InvokerHelper.clearCallback();
        }
        callback.onSuccess(defaultResult);
        return NO_RETURN_RESPONSE;
    }

    private static InvocationResponse callBackOnfailure(InvokerContext context, Exception exception) {
        InvocationCallback callback = context.getInvokerConfig().getCallback();
        InvocationCallback tlCallback = InvokerHelper.getCallback();
        if (tlCallback != null) {
            callback = tlCallback;
            InvokerHelper.clearCallback();
        }
        callback.onFailure(exception);
        InvocationResponse response = NO_RETURN_RESPONSE;
        /*ExceptionManager.INSTANCE.logRpcException(null, context.getInvokerConfig().getUrl(), context.getMethodName(),
                "callback degraded", exception, null, response, MonitorLoader.getMonitor().getCurrentCallTransaction());*/
        return response;
    }

    private static Object getGroovyMockProxy(String key, InvokerContext context, DegradeAction action) {
        Object interfaceProxy = groovyMocks.get(key);

        if (interfaceProxy == null) {
            interfaceProxy = MockInvocationUtils.getProxy(context.getInvokerConfig(),
                    new GroovyScriptInvocationProxy(action.getGroovyScript()));
            Object oldInterfaceProxy = groovyMocks.putIfAbsent(key, interfaceProxy);
            if (oldInterfaceProxy != null) {
                interfaceProxy = oldInterfaceProxy;
            }
        }

        return interfaceProxy;
    }

    public static Map<String, DegradeAction> getDegradeMethodActions() {
        return degradeMethodActions;
    }

    public static class DegradeAction implements Serializable {
        private boolean throwException = false;
        private Object returnObj;
        private boolean useMockClass = false;
        private boolean useGroovyScript = false;
        private Script groovyScript;
        private boolean enable = true;

        public Script getGroovyScript() {
            return groovyScript;
        }

        public void setGroovyScript(Script groovyScript) {
            this.groovyScript = groovyScript;
        }

        public boolean isUseGroovyScript() {
            return useGroovyScript;
        }

        public void setUseGroovyScript(boolean useGroovyScript) {
            this.useGroovyScript = useGroovyScript;
        }

        public boolean isUseMockClass() {
            return useMockClass;
        }

        public void setUseMockClass(boolean useMockClass) {
            this.useMockClass = useMockClass;
        }

        public boolean isThrowException() {
            return throwException;
        }

        public void setThrowException(boolean throwException) {
            this.throwException = throwException;
        }

        public Object getReturnObj() {
            return returnObj;
        }

        public void setReturnObj(Object returnObj) {
            this.returnObj = returnObj;
        }

        public void setEnable(boolean enable) {
            this.enable = enable;
        }

        public boolean getEnable() {
            return enable;
        }
    }
}