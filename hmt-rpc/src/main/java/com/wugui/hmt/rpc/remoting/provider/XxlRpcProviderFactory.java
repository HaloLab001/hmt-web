package com.wugui.hmt.rpc.remoting.provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wugui.hmt.rpc.registry.ServiceRegistry;
import com.wugui.hmt.rpc.remoting.net.Server;
import com.wugui.hmt.rpc.remoting.net.impl.netty.server.NettyServer;
import com.wugui.hmt.rpc.remoting.net.params.BaseCallback;
import com.wugui.hmt.rpc.remoting.net.params.XxlRpcRequest;
import com.wugui.hmt.rpc.remoting.net.params.XxlRpcResponse;
import com.wugui.hmt.rpc.serialize.Serializer;
import com.wugui.hmt.rpc.serialize.impl.HessianSerializer;
import com.wugui.hmt.rpc.util.IpUtil;
import com.wugui.hmt.rpc.util.NetUtil;
import com.wugui.hmt.rpc.util.ThrowableUtil;
import com.wugui.hmt.rpc.util.XxlRpcException;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;


/*-------------------------------------------------------------------------
 *
 * XxlRpcProviderFactory.java
 *  provider
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
 *    /hmt-rpc/src/main/java/com/wugui/hmt/rpc/remoting/provider/XxlRpcProviderFactory.java
 *
 *-----------------------------------------------
 */
public class XxlRpcProviderFactory {
	private static final Logger logger = LoggerFactory.getLogger(XxlRpcProviderFactory.class);

	// ---------------------- config ----------------------

	private Class<? extends Server> server = NettyServer.class;
	private Class<? extends Serializer> serializer = HessianSerializer.class;

	private int corePoolSize = 60;
	private int maxPoolSize = 300;

	private String ip = null;					// for registry
	private int port = 7080;					// default port
	private String accessToken = null;

	private Class<? extends ServiceRegistry> serviceRegistry = null;
	private Map<String, String> serviceRegistryParam = null;

	// set
	public void setServer(Class<? extends Server> server) {
		this.server = server;
	}
	public void setSerializer(Class<? extends Serializer> serializer) {
		this.serializer = serializer;
	}
	public void setCorePoolSize(int corePoolSize) {
		this.corePoolSize = corePoolSize;
	}
	public void setMaxPoolSize(int maxPoolSize) {
		this.maxPoolSize = maxPoolSize;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public void setPort(int port) {
		this.port = port;
	}
	public void setAccessToken(String accessToken) {
		this.accessToken = accessToken;
	}

	public void setServiceRegistry(Class<? extends ServiceRegistry> serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
	}

	public void setServiceRegistryParam(Map<String, String> serviceRegistryParam) {
		this.serviceRegistryParam = serviceRegistryParam;
	}

	// get
	public Serializer getSerializerInstance() {
		return serializerInstance;
	}
	public int getPort() {
		return port;
	}
	public int getCorePoolSize() {
		return corePoolSize;
	}
	public int getMaxPoolSize() {
		return maxPoolSize;
	}

	// ---------------------- start / stop ----------------------

	private Server serverInstance;
	private Serializer serializerInstance;
	private ServiceRegistry serviceRegistryInstance;
	private String serviceAddress;

	public void start() throws Exception {

		// valid
		if (this.server == null) {
			throw new XxlRpcException("xxl-rpc provider server missing.");
		}
		if (this.serializer==null) {
			throw new XxlRpcException("xxl-rpc provider serializer missing.");
		}
		if (!(this.corePoolSize>0 && this.maxPoolSize>0 && this.maxPoolSize>=this.corePoolSize)) {
			this.corePoolSize = 60;
			this.maxPoolSize = 300;
		}
		if (this.ip == null) {
			this.ip = IpUtil.getIp();
		}
		if (this.port <= 0) {
			this.port = 7080;
		}
		if (NetUtil.isPortUsed(this.port)) {
			throw new XxlRpcException("xxl-rpc provider port["+ this.port +"] is used.");
		}

		// init serializerInstance
		this.serializerInstance = serializer.newInstance();

		// start server
		serviceAddress = IpUtil.getIpPort(this.ip, port);
		serverInstance = server.newInstance();
		serverInstance.setStartedCallback(new BaseCallback() {		// serviceRegistry started
			@Override
			public void run() throws Exception {
				// start registry
				if (serviceRegistry != null) {
					serviceRegistryInstance = serviceRegistry.newInstance();
					serviceRegistryInstance.start(serviceRegistryParam);
					if (serviceData.size() > 0) {
						serviceRegistryInstance.registry(serviceData.keySet(), serviceAddress);
					}
				}
			}
		});
		serverInstance.setStopedCallback(new BaseCallback() {		// serviceRegistry stoped
			@Override
			public void run() {
				// stop registry
				if (serviceRegistryInstance != null) {
					if (serviceData.size() > 0) {
						serviceRegistryInstance.remove(serviceData.keySet(), serviceAddress);
					}
					serviceRegistryInstance.stop();
					serviceRegistryInstance = null;
				}
			}
		});
		serverInstance.start(this);
	}

	public void  stop() throws Exception {
		// stop server
		serverInstance.stop();
	}


	// ---------------------- server invoke ----------------------

	/**
	 * init local rpc service map
	 */
	private Map<String, Object> serviceData = new HashMap<String, Object>();
	public Map<String, Object> getServiceData() {
		return serviceData;
	}

	/**
	 * make service key
	 *
	 * @param iface
	 * @param version
	 * @return
	 */
	public static String makeServiceKey(String iface, String version){
		String serviceKey = iface;
		if (version!=null && version.trim().length()>0) {
			serviceKey += "#".concat(version);
		}
		return serviceKey;
	}

	/**
	 * add service
	 *
	 * @param iface
	 * @param version
	 * @param serviceBean
	 */
	public void addService(String iface, String version, Object serviceBean){
		String serviceKey = makeServiceKey(iface, version);
		serviceData.put(serviceKey, serviceBean);

		logger.info(">>>>>>>>>>> xxl-rpc, provider factory add service success. serviceKey = {}, serviceBean = {}", serviceKey, serviceBean.getClass());
	}

	/**
	 * invoke service
	 *
	 * @param xxlRpcRequest
	 * @return
	 */
	public XxlRpcResponse invokeService(XxlRpcRequest xxlRpcRequest) {

		//  make response
		XxlRpcResponse xxlRpcResponse = new XxlRpcResponse();
		xxlRpcResponse.setRequestId(xxlRpcRequest.getRequestId());

		// match service bean
		String serviceKey = makeServiceKey(xxlRpcRequest.getClassName(), xxlRpcRequest.getVersion());
		Object serviceBean = serviceData.get(serviceKey);

		// valid
		if (serviceBean == null) {
			xxlRpcResponse.setErrorMsg("The serviceKey["+ serviceKey +"] not found.");
			return xxlRpcResponse;
		}

		if (System.currentTimeMillis() - xxlRpcRequest.getCreateMillisTime() > 3*60*1000) {
			xxlRpcResponse.setErrorMsg("The timestamp difference between admin and executor exceeds the limit.");
			return xxlRpcResponse;
		}
		if (accessToken!=null && accessToken.trim().length()>0 && !accessToken.trim().equals(xxlRpcRequest.getAccessToken())) {
			xxlRpcResponse.setErrorMsg("The access token[" + xxlRpcRequest.getAccessToken() + "] is wrong.");
			return xxlRpcResponse;
		}

		try {
			// invoke
			Class<?> serviceClass = serviceBean.getClass();
			String methodName = xxlRpcRequest.getMethodName();
			Class<?>[] parameterTypes = xxlRpcRequest.getParameterTypes();
			Object[] parameters = xxlRpcRequest.getParameters();

            Method method = serviceClass.getMethod(methodName, parameterTypes);
            method.setAccessible(true);
			Object result = method.invoke(serviceBean, parameters);

			/*FastClass serviceFastClass = FastClass.create(serviceClass);
			FastMethod serviceFastMethod = serviceFastClass.getMethod(methodName, parameterTypes);
			Object result = serviceFastMethod.invoke(serviceBean, parameters);*/

			xxlRpcResponse.setResult(result);
		} catch (Throwable t) {
			// catch error
			logger.error("xxl-rpc provider invokeService error.", t);
			xxlRpcResponse.setErrorMsg(ThrowableUtil.toString(t));
		}

		return xxlRpcResponse;
	}

}
