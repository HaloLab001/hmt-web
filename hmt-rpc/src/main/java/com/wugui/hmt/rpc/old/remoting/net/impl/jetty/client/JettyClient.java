/*-------------------------------------------------------------------------
 *
 * JettyClient.java
 * JettyClient类
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
 *    /hmt-rpc/src/main/java/com/wugui/hmt/rpc/old/remoting/net/impl/jetty/client/JettyClient.java
 *
 *-----------------------------------------------
 */
package com.wugui.hmt.rpc.old.remoting.net.impl.jetty.client;//package com.xxl.rpc.remoting.net.impl.jetty.client;
//
//import com.xxl.rpc.remoting.invoker.XxlRpcInvokerFactory;
//import com.xxl.rpc.remoting.net.Client;
//import com.xxl.rpc.remoting.net.params.BaseCallback;
//import com.xxl.rpc.remoting.net.params.XxlRpcRequest;
//import com.xxl.rpc.remoting.net.params.XxlRpcResponse;
//import com.xxl.rpc.util.ThrowableUtil;
//import com.xxl.rpc.util.XxlRpcException;
//import org.eclipse.jetty.client.HttpClient;
//import org.eclipse.jetty.client.api.Request;
//import org.eclipse.jetty.client.api.Result;
//import org.eclipse.jetty.client.util.BufferingResponseListener;
//import org.eclipse.jetty.client.util.BytesContentProvider;
//import org.eclipse.jetty.http.HttpMethod;
//import org.eclipse.jetty.http.HttpStatus;
//import org.eclipse.jetty.util.thread.QueuedThreadPool;
//
//import java.util.concurrent.TimeUnit;
//
///**
// * jetty client
// *
// * @author xuxueli 2015-11-24 22:25:15
// */
//public class JettyClient extends Client {
//
//	@Override
//	public void asyncSend(String address, XxlRpcRequest xxlRpcRequest) throws Exception {
//		// do invoke
//		postRequestAsync(address, xxlRpcRequest);
//	}
//
//    /**
//     * post request (async)
//     *
//     * @param address
//     * @return
//     * @throws Exception
//     */
//    private void postRequestAsync(String address, XxlRpcRequest xxlRpcRequest) throws Exception {
//
//		// reqURL
//		String reqURL = address;
//		if (!address.toLowerCase().startsWith("http")) {
//			reqURL = "http://" + address;	// IP:PORT, need parse to url
//		}
//
//		// serialize request
//		byte[] requestBytes = xxlRpcReferenceBean.getSerializer().serialize(xxlRpcRequest);
//
//        // httpclient
//        HttpClient httpClient = getJettyHttpClient(xxlRpcReferenceBean.getInvokerFactory());
//
//        // request
//        Request request = httpClient.newRequest(reqURL);
//        request.method(HttpMethod.POST);
//        request.timeout(xxlRpcReferenceBean.getTimeout() + 500, TimeUnit.MILLISECONDS);		// async, not need timeout
//        request.content(new BytesContentProvider(requestBytes));
//
//        // invoke
//        request.send(new BufferingResponseListener(5 * 1024 * 1024) {	// maxLength = 5M
//			@Override
//			public void onComplete(Result result) {
//
//			    try {
//
//                    // valid status
//                    if (result.isFailed()) {
//                        throw new XxlRpcException(result.getResponseFailure());
//                    }
//
//                    // valid HttpStatus
//                    if (result.getResponse().getStatus() != HttpStatus.OK_200) {
//                        throw new XxlRpcException("xxl-rpc remoting request fail, http HttpStatus["+ result.getResponse().getStatus() +"] invalid.");
//                    }
//
//                    // valid response bytes
//                    byte[] responseBytes = getContent();
//                    if (responseBytes == null || responseBytes.length==0) {
//                        throw new XxlRpcException("xxl-rpc remoting request fail, response bytes is empty.");
//                    }
//
//                    // deserialize response
//                    XxlRpcResponse xxlRpcResponse = (XxlRpcResponse) xxlRpcReferenceBean.getSerializer().deserialize(responseBytes, XxlRpcResponse.class);
//
//                    // notify response
//					xxlRpcReferenceBean.getInvokerFactory().notifyInvokerFuture(xxlRpcResponse.getRequestId(), xxlRpcResponse);
//
//                } catch (Exception e){
//
//			        // fail, request finish, remove request
//			        if (result.getRequest().getContent() instanceof BytesContentProvider) {
//			            try {
//                            BytesContentProvider requestCp = (BytesContentProvider) result.getRequest().getContent();
//                            XxlRpcRequest requestTmp = (XxlRpcRequest) xxlRpcReferenceBean.getSerializer().deserialize(requestCp.iterator().next().array(), XxlRpcRequest.class);
//
//							// error msg
//							String errorMsg = null;
//							if (e instanceof XxlRpcException) {
//								XxlRpcException rpcException = (XxlRpcException) e;
//								if (rpcException.getCause() != null) {
//									errorMsg = ThrowableUtil.toString(rpcException.getCause());
//								} else {
//									errorMsg = rpcException.getMessage();
//								}
//							} else {
//								errorMsg = ThrowableUtil.toString(e);
//							}
//
//							//  make response
//							XxlRpcResponse xxlRpcResponse = new XxlRpcResponse();
//							xxlRpcResponse.setRequestId(requestTmp.getRequestId());
//							xxlRpcResponse.setErrorMsg(errorMsg);
//
//							// notify response
//							xxlRpcReferenceBean.getInvokerFactory().notifyInvokerFuture(xxlRpcResponse.getRequestId(), xxlRpcResponse);
//
//                        } catch (Exception e2) {
//                            logger.info(">>>>>>>>>>> xxl-rpc, remoting request error, and callback error: " + e2.getMessage());
//							logger.info(e.getMessage(), e);
//                        }
//                    } else {
//						logger.info(">>>>>>>>>>> xxl-rpc, remoting request error.", e);
//					}
//
//                }
//
//
//			}
//		});
//    }
//
//	/**
//	 * make jetty http client
//	 *
//	 * @return
//	 * @throws Exception
//	 */
//	private static HttpClient jettyHttpClient;		// (static) alread addStopCallBack
//	public static HttpClient getJettyHttpClient(final XxlRpcInvokerFactory xxlRpcInvokerFactory) throws Exception {
//
//		// get
//		if (jettyHttpClient != null) {
//			return jettyHttpClient;
//		}
//
//		// init jetty cilent, avoid repeat init
//		synchronized (JettyClient.class) {
//
//			// re-get
//			if (jettyHttpClient != null) {
//				return jettyHttpClient;
//			}
//
//
//			// init jettp httpclient
//			jettyHttpClient = new HttpClient();
//			jettyHttpClient.setFollowRedirects(false);	                // avoid redirect-302
//			jettyHttpClient.setExecutor(new QueuedThreadPool());		// default maxThreads 200, minThreads 8
//			jettyHttpClient.setMaxConnectionsPerDestination(10000);	    // limit conn per desc
//			jettyHttpClient.start();						            // start
//
//			// stop callback
//			xxlRpcInvokerFactory.addStopCallBack(new BaseCallback() {
//				@Override
//				public void run() throws Exception {
//					if (jettyHttpClient != null) {
//						jettyHttpClient.stop();
//						jettyHttpClient = null;
//					}
//				}
//			});
//		}
//
//		return jettyHttpClient;
//	}
//
//	/*@Override
//	public XxlRpcResponse send(String address, XxlRpcRequest xxlRpcRequest) throws Exception {
//
//		// reqURL
//		if (!address.toLowerCase().startsWith("http")) {
//			address = "http://" + address + "/";	// IP:PORT, need parse to url
//		}
//
//		// serialize request
//		byte[] requestBytes = xxlRpcReferenceBean.getSerializer().serialize(xxlRpcRequest);
//
//		// remote invoke
//		byte[] responseBytes = postRequest(address, requestBytes, xxlRpcReferenceBean.getTimeout());
//
//		// deserialize response
//		return (XxlRpcResponse) xxlRpcReferenceBean.getSerializer().deserialize(responseBytes, XxlRpcResponse.class);
//
//	}*/
//
//	/**
//	 * post request
//	 */
//	/*private static byte[] postRequest(String reqURL, byte[] data, long timeout) throws Exception {
//
//		// httpclient
//		HttpClient httpClient = new HttpClient();
//		httpClient.setFollowRedirects(false);	// Configure HttpClient, for example:
//		httpClient.start();						// Start HttpClient
//
//		// request
//		Request request = httpClient.newRequest(reqURL);
//		request.method(HttpMethod.POST);
//		request.timeout(timeout, TimeUnit.MILLISECONDS);
//		request.content(new BytesContentProvider(data));
//
//		// invoke
//		ContentResponse response = request.send();
//		if (response.getStatus() != HttpStatus.OK_200) {
//			throw new RuntimeException("xxl-rpc remoting request fail, http HttpStatus["+ response.getStatus() +"] invalid.");
//		}
//
//		// result
//		byte[] responseBytes = response.getContent();
//		if (responseBytes == null || responseBytes.length==0) {
//			throw new RuntimeException("xxl-rpc remoting request fail, response bytes is empty.");
//		}
//
//		return responseBytes;
//	}*/
//
//
//}
