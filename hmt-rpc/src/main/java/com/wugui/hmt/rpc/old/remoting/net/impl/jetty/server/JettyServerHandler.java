/*-------------------------------------------------------------------------
 *
 * JettyServerHandler.java
 * JettyServerHandler类
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
 *    /hmt-rpc/src/main/java/com/wugui/hmt/rpc/old/remoting/net/impl/jetty/server/JettyServerHandler.java
 *
 *-----------------------------------------------
 */
package com.wugui.hmt.rpc.old.remoting.net.impl.jetty.server;//package com.xxl.rpc.remoting.net.impl.jetty.server;
//
//import com.xxl.rpc.remoting.net.params.XxlRpcRequest;
//import com.xxl.rpc.remoting.net.params.XxlRpcResponse;
//import com.xxl.rpc.remoting.provider.XxlRpcProviderFactory;
//import com.xxl.rpc.util.ThrowableUtil;
//import com.xxl.rpc.util.XxlRpcException;
//import org.eclipse.jetty.server.Request;
//import org.eclipse.jetty.server.handler.AbstractHandler;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import javax.servlet.ServletException;
//import javax.servlet.http.HttpServletRequest;
//import javax.servlet.http.HttpServletResponse;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//
///**
// * jetty handler
// * @author xuxueli 2015-11-19 22:32:36
// */
//public class JettyServerHandler extends AbstractHandler {
//	private static Logger logger = LoggerFactory.getLogger(JettyServerHandler.class);
//
//
//	private XxlRpcProviderFactory xxlRpcProviderFactory;
//	public JettyServerHandler(final XxlRpcProviderFactory xxlRpcProviderFactory) {
//		this.xxlRpcProviderFactory = xxlRpcProviderFactory;
//	}
//
//
//	@Override
//	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
//
//		if ("/services".equals(target)) {	// services mapping
//
//			StringBuffer stringBuffer = new StringBuffer("<ui>");
//			for (String serviceKey: xxlRpcProviderFactory.getServiceData().keySet()) {
//				stringBuffer.append("<li>").append(serviceKey).append(": ").append(xxlRpcProviderFactory.getServiceData().get(serviceKey)).append("</li>");
//			}
//			stringBuffer.append("</ui>");
//
//			writeResponse(baseRequest, response, stringBuffer.toString().getBytes());
//			return;
//		} else {	// default remoting mapping
//
//			// request parse
//			XxlRpcRequest xxlRpcRequest = null;
//			try {
//
//				xxlRpcRequest = parseRequest(request);
//			} catch (Exception e) {
//				writeResponse(baseRequest, response, ThrowableUtil.toString(e).getBytes());
//				return;
//			}
//
//			// invoke
//			XxlRpcResponse xxlRpcResponse = xxlRpcProviderFactory.invokeService(xxlRpcRequest);
//
//			// response-serialize + response-write
//			byte[] responseBytes = xxlRpcProviderFactory.getSerializer().serialize(xxlRpcResponse);
//			writeResponse(baseRequest, response, responseBytes);
//		}
//
//	}
//
//	/**
//	 * write response
//	 */
//	private void writeResponse(Request baseRequest, HttpServletResponse response, byte[] responseBytes) throws IOException {
//
//		response.setContentType("text/html;charset=UTF-8");
//		response.setStatus(HttpServletResponse.SC_OK);
//		baseRequest.setHandled(true);
//
//		OutputStream out = response.getOutputStream();
//		out.write(responseBytes);
//		out.flush();
//	}
//
//	/**
//	 * parse request
//	 */
//	private XxlRpcRequest parseRequest(HttpServletRequest request) throws Exception {
//		// deserialize request
//		byte[] requestBytes = readBytes(request);
//		if (requestBytes == null || requestBytes.length==0) {
//			throw new XxlRpcException("xxl-rpc request data is empty.");
//		}
//		XxlRpcRequest rpcXxlRpcRequest = (XxlRpcRequest) xxlRpcProviderFactory.getSerializer().deserialize(requestBytes, XxlRpcRequest.class);
//		return rpcXxlRpcRequest;
//	}
//
//	/**
//	 * read bytes from http request
//	 *
//	 * @param request
//	 * @return
//	 * @throws IOException
//	 */
//	public static final byte[] readBytes(HttpServletRequest request) throws IOException {
//		request.setCharacterEncoding("UTF-8");
//		int contentLen = request.getContentLength();
//		InputStream is = request.getInputStream();
//		if (contentLen > 0) {
//			int readLen = 0;
//			int readLengthThisTime = 0;
//			byte[] message = new byte[contentLen];
//			try {
//				while (readLen != contentLen) {
//					readLengthThisTime = is.read(message, readLen, contentLen - readLen);
//					if (readLengthThisTime == -1) {
//						break;
//					}
//					readLen += readLengthThisTime;
//				}
//				return message;
//			} catch (IOException e) {
//				logger.error(e.getMessage(), e);
//			}
//		}
//		return new byte[] {};
//	}
//
//}
