package com.wugui.hmt.rpc.remoting.invoker.reference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wugui.hmt.rpc.remoting.invoker.XxlRpcInvokerFactory;
import com.wugui.hmt.rpc.remoting.invoker.call.CallType;
import com.wugui.hmt.rpc.remoting.invoker.call.XxlRpcInvokeCallback;
import com.wugui.hmt.rpc.remoting.invoker.call.XxlRpcInvokeFuture;
import com.wugui.hmt.rpc.remoting.invoker.generic.XxlRpcGenericService;
import com.wugui.hmt.rpc.remoting.invoker.route.LoadBalance;
import com.wugui.hmt.rpc.remoting.net.Client;
import com.wugui.hmt.rpc.remoting.net.impl.netty.client.NettyClient;
import com.wugui.hmt.rpc.remoting.net.params.XxlRpcFutureResponse;
import com.wugui.hmt.rpc.remoting.net.params.XxlRpcRequest;
import com.wugui.hmt.rpc.remoting.net.params.XxlRpcResponse;
import com.wugui.hmt.rpc.remoting.provider.XxlRpcProviderFactory;
import com.wugui.hmt.rpc.serialize.Serializer;
import com.wugui.hmt.rpc.serialize.impl.HessianSerializer;
import com.wugui.hmt.rpc.util.ClassUtil;
import com.wugui.hmt.rpc.util.XxlRpcException;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;


/*-------------------------------------------------------------------------
 *
 * XxlRpcReferenceBean.java
 *  rpc reference bean, use by api
 *
 * 版权所有 (c) 2019-2024, 易景科技保留所有权利。
 * Copyright (c) 2019-2024, Halo Tech Co.,Ltd. All rights reserved.
 *
 * 易景科技是Halo Database、Halo Database Management System、羲和数据
 * 库、羲和数据库管理系统（后面简称 Halo ）软件的发明人同时也为知识产权权
 * 利人。Halo 软件的知识产权，以及与本软件相关的所有信息内容（包括但不限
 * 于文字、图片、音频、视频、图表、界面设计、版面框架、有关数据或电子文档等）
 * 均受中华人民共和国法律法规和相应的国际条约保护，易景科技享有上述知识产
 * 权，但相关权利人依照法律规定应享有的权利除外。未免疑义，本条所指的“知识
 * 产权”是指任何及所有基于 Halo 软件产生的：（a）版权、商标、商号、域名、与
 * 商标和商号相关的商誉、设计和专利；与创新、技术诀窍、商业秘密、保密技术、非
 * 技术信息相关的权利；（b）人身权、掩模作品权、署名权和发表权；以及（c）在
 * 本协议生效之前已存在或此后出现在世界任何地方的其他工业产权、专有权、与“知
 * 识产权”相关的权利，以及上述权利的所有续期和延长，无论此类权利是否已在相
 * 关法域内的相关机构注册。
 *
 * This software and related documentation are provided under a license
 * agreement containing restrictions on use and disclosure and are 
 * protected by intellectual property laws. Except as expressly permitted
 * in your license agreement or allowed by law, you may not use, copy, 
 * reproduce, translate, broadcast, modify, license, transmit, distribute,
 * exhibit, perform, publish, or display any part, in any form, or by any
 * means. Reverse engineering, disassembly, or decompilation of this 
 * software, unless required by law for interoperability, is prohibited.
 *
 * This software is developed for general use in a variety of
 * information management applications. It is not developed or intended
 * for use in any inherently dangerous applications, including applications
 * that may create a risk of personal injury. If you use this software or
 * in dangerous applications, then you shall be responsible to take all
 * appropriate fail-safe, backup, redundancy, and other measures to ensure
 * its safe use. Halo Corporation and its affiliates disclaim any 
 * liability for any damages caused by use of this software in dangerous
 * applications.
 *
 *
 * IDENTIFICATION
 *    /hmt-rpc/src/main/java/com/wugui/hmt/rpc/remoting/invoker/reference/XxlRpcReferenceBean.java
 *
 *-----------------------------------------------
 */
public class XxlRpcReferenceBean {
    private static final Logger logger = LoggerFactory.getLogger(XxlRpcReferenceBean.class);
    // [tips01: save 30ms/100invoke. why why why??? with this logger, it can save lots of time.]


    // ---------------------- config ----------------------

    private Class<? extends Client> client = NettyClient.class;
    private Class<? extends Serializer> serializer = HessianSerializer.class;
    private CallType callType = CallType.SYNC;
    private LoadBalance loadBalance = LoadBalance.ROUND;

    private Class<?> iface = null;
    private String version = null;

    private long timeout = 10000;

    private String address = null;
    private String accessToken = null;

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
        if (this.callType == null) {
            throw new XxlRpcException("xxl-rpc reference callType missing.");
        }
        if (this.loadBalance == null) {
            throw new XxlRpcException("xxl-rpc reference loadBalance missing.");
        }
        if (this.iface == null) {
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

    public Object getObject() throws Exception {

        // initClient
        initClient();

        // newProxyInstance
        return Proxy.newProxyInstance(Thread.currentThread()
                        .getContextClassLoader(), new Class[]{iface},
                (proxy, method, args) -> {

                    // method param
                    String className = method.getDeclaringClass().getName();    // iface.getName()
                    String varsion_ = version;
                    String methodName = method.getName();
                    Class<?>[] parameterTypes = method.getParameterTypes();
                    Object[] parameters = args;

                    // filter for generic
                    if (className.equals(XxlRpcGenericService.class.getName()) && methodName.equals("invoke")) {

                        Class<?>[] paramTypes = null;
                        if (args[3] != null) {
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

                    // filter method like "Object.toString()"
                    if (className.equals(Object.class.getName())) {
                        logger.info(">>>>>>>>>>> xxl-rpc proxy class-method not support [{}#{}]", className, methodName);
                        throw new XxlRpcException("xxl-rpc proxy class-method not support");
                    }

                    // address
                    String finalAddress = address;
                    if (finalAddress == null || finalAddress.trim().length() == 0) {
                        if (invokerFactory != null && invokerFactory.getServiceRegistry() != null) {
                            // discovery
                            String serviceKey = XxlRpcProviderFactory.makeServiceKey(className, varsion_);
                            TreeSet<String> addressSet = invokerFactory.getServiceRegistry().discovery(serviceKey);
                            // load balance
                            if (addressSet == null || addressSet.size() == 0) {
                                // pass
                            } else if (addressSet.size() == 1) {
                                finalAddress = addressSet.first();
                            } else {
                                finalAddress = loadBalance.xxlRpcInvokerRouter.route(serviceKey, addressSet);
                            }

                        }
                    }
                    if (finalAddress == null || finalAddress.trim().length() == 0) {
                        throw new XxlRpcException("xxl-rpc reference bean[" + className + "] address empty");
                    }

                    // request
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
                    if (CallType.SYNC == callType) {
                        // future-response set
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

                            throw (e instanceof XxlRpcException) ? e : new XxlRpcException(e);
                        } finally {
                            // future-response remove
                            futureResponse.removeInvokerFuture();
                        }
                    } else if (CallType.FUTURE == callType) {
                        // future-response set
                        XxlRpcFutureResponse futureResponse = new XxlRpcFutureResponse(invokerFactory, xxlRpcRequest, null);
                        try {
                            // invoke future set
                            XxlRpcInvokeFuture invokeFuture = new XxlRpcInvokeFuture(futureResponse);
                            XxlRpcInvokeFuture.setFuture(invokeFuture);

// do invoke
                            clientInstance.asyncSend(finalAddress, xxlRpcRequest);

                            return null;
                        } catch (Exception e) {
                            logger.info(">>>>>>>>>>> xxl-rpc, invoke error, address:{}, XxlRpcRequest{}", finalAddress, xxlRpcRequest);

                            // future-response remove
                            futureResponse.removeInvokerFuture();

                            throw (e instanceof XxlRpcException) ? e : new XxlRpcException(e);
                        }

                    } else if (CallType.CALLBACK == callType) {

                        // get callback
                        XxlRpcInvokeCallback finalInvokeCallback = invokeCallback;
                        XxlRpcInvokeCallback threadInvokeCallback = XxlRpcInvokeCallback.getCallback();
                        if (threadInvokeCallback != null) {
                            finalInvokeCallback = threadInvokeCallback;
                        }
                        if (finalInvokeCallback == null) {
                            throw new XxlRpcException("xxl-rpc XxlRpcInvokeCallback（CallType=" + CallType.CALLBACK.name() + "） cannot be null.");
                        }

                        // future-response set
                        XxlRpcFutureResponse futureResponse = new XxlRpcFutureResponse(invokerFactory, xxlRpcRequest, finalInvokeCallback);
                        try {
                            clientInstance.asyncSend(finalAddress, xxlRpcRequest);
                        } catch (Exception e) {
                            logger.info(">>>>>>>>>>> xxl-rpc, invoke error, address:{}, XxlRpcRequest{}", finalAddress, xxlRpcRequest);

                            // future-response remove
                            futureResponse.removeInvokerFuture();

                            throw (e instanceof XxlRpcException) ? e : new XxlRpcException(e);
                        }

                        return null;
                    } else if (CallType.ONEWAY == callType) {
                        clientInstance.asyncSend(finalAddress, xxlRpcRequest);
                        return null;
                    } else {
                        throw new XxlRpcException("xxl-rpc callType[" + callType + "] invalid");
                    }

                });
    }

}
