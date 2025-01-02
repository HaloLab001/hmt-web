/*-------------------------------------------------------------------------
 *
 * MinaServer.java
 * MinaServer类
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
 *    /hmt-rpc/src/main/java/com/wugui/hmt/rpc/old/remoting/net/impl/mina/server/MinaServer.java
 *
 *-----------------------------------------------
 */
package com.wugui.hmt.rpc.old.remoting.net.impl.mina.server;//package com.xxl.rpc.remoting.net.impl.mina.server;
//
//import com.xxl.rpc.remoting.net.Server;
//import com.xxl.rpc.remoting.net.impl.mina.codec.MinaDecoder;
//import com.xxl.rpc.remoting.net.impl.mina.codec.MinaEncoder;
//import com.xxl.rpc.remoting.net.params.XxlRpcRequest;
//import com.xxl.rpc.remoting.net.params.XxlRpcResponse;
//import com.xxl.rpc.remoting.provider.XxlRpcProviderFactory;
//import com.xxl.rpc.util.ThreadPoolUtil;
//import org.apache.mina.core.session.IdleStatus;
//import org.apache.mina.core.session.IoSession;
//import org.apache.mina.filter.codec.ProtocolCodecFactory;
//import org.apache.mina.filter.codec.ProtocolCodecFilter;
//import org.apache.mina.filter.codec.ProtocolDecoder;
//import org.apache.mina.filter.codec.ProtocolEncoder;
//import org.apache.mina.filter.executor.ExecutorFilter;
//import org.apache.mina.transport.socket.SocketSessionConfig;
//import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
//
//import java.net.InetSocketAddress;
//import java.util.concurrent.Executors;
//import java.util.concurrent.ThreadPoolExecutor;
//
///**
// * mina rpc server
// *
// * 		<!-- mina -->
// * 		<dependency>
// * 			<groupId>org.apache.mina</groupId>
// * 			<artifactId>mina-core</artifactId>
// * 			<version>${mina.version}</version>
// * 			<scope>provided</scope>
// * 		</dependency>
// *
// * @author xuxueli 2015-11-14 17:22:09
// */
//public class MinaServer extends Server {
//
//	private Thread thread;
//
//	@Override
//	public void start(final XxlRpcProviderFactory xxlRpcProviderFactory) throws Exception {
//
//        thread = new Thread(new Runnable() {
//			@Override
//			public void run() {
//
//				// param
//				final ThreadPoolExecutor serverHandlerPool = ThreadPoolUtil.makeServerThreadPool(
//						MinaServer.class.getSimpleName(),
//						xxlRpcProviderFactory.getCorePoolSize(),
//						xxlRpcProviderFactory.getMaxPoolSize());
//				NioSocketAcceptor acceptor = new NioSocketAcceptor();
//
//				try {
//
//					// heartbeat
//					/*KeepAliveFilter heartBeat = new KeepAliveFilter(new KeepAliveMessageFactory() {
//						@Override
//						public boolean isRequest(IoSession ioSession, Object message) {
//							return Beat.BEAT_ID.equalsIgnoreCase(((XxlRpcRequest) message).getRequestId());
//						}
//						@Override
//						public boolean isResponse(IoSession ioSession, Object message) {
//							return Beat.BEAT_ID.equalsIgnoreCase(((XxlRpcResponse) message).getRequestId());
//						}
//						@Override
//						public Object getRequest(IoSession ioSession) {
//							return Beat.BEAT_PING;
//						}
//						@Override
//						public Object getResponse(IoSession ioSession, Object request) {
//							return Beat.BEAT_PONG;
//						}
//					}, IdleStatus.BOTH_IDLE, KeepAliveRequestTimeoutHandler.CLOSE);
//					heartBeat.setForwardEvent(true);
//					heartBeat.setRequestInterval(10);
//					heartBeat.setRequestTimeout(10);*/
//
//					// start server
//					//acceptor.getFilterChain().addLast("heartbeat", heartBeat);
//					acceptor.getFilterChain().addLast("threadPool", new ExecutorFilter(Executors.newCachedThreadPool()));
//					acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new ProtocolCodecFactory() {
//						@Override
//						public ProtocolEncoder getEncoder(IoSession session) throws Exception {
//							return new MinaEncoder(XxlRpcResponse.class, xxlRpcProviderFactory.getSerializer());
//						}
//						@Override
//						public ProtocolDecoder getDecoder(IoSession session) throws Exception {
//							return new MinaDecoder(XxlRpcRequest.class, xxlRpcProviderFactory.getSerializer());
//						}
//					}));
//					acceptor.setHandler(new MinaServerHandler(xxlRpcProviderFactory, serverHandlerPool));
//
//					SocketSessionConfig socketSessionConfig = acceptor.getSessionConfig();
//					socketSessionConfig.setTcpNoDelay(true);
//					socketSessionConfig.setKeepAlive(true);
//					//config.setReuseAddress(true);
//					socketSessionConfig.setSoLinger(-1);
//					socketSessionConfig.setIdleTime(IdleStatus.BOTH_IDLE, 60);
//
//					acceptor.bind(new InetSocketAddress(xxlRpcProviderFactory.getPort()));
//
//					logger.info(">>>>>>>>>>> xxl-rpc remoting server start success, nettype = {}, port = {}", MinaServer.class.getName(), xxlRpcProviderFactory.getPort());
//					onStarted();
//
//					while (!Thread.currentThread().isInterrupted()) {
//						Thread.sleep(1);
//					}
//				} catch (Exception e) {
//					if (e instanceof InterruptedException) {
//						logger.info(">>>>>>>>>>> xxl-rpc remoting server stop.");
//					} else {
//						logger.error(">>>>>>>>>>> xxl-rpc remoting server error.", e);
//					}
//				} finally {
//
//					// stop
//					try {
//						serverHandlerPool.shutdown();
//					} catch (Exception e) {
//						logger.error(e.getMessage(), e);
//					}
//					try {
//						if (acceptor.isActive()) {
//							acceptor.unbind();
//							acceptor.dispose();
//						}
//					} catch (Exception e) {
//						logger.error(e.getMessage(), e);
//					}
//
//				}
//			}
//		});
//        thread.setDaemon(true);
//        thread.start();
//
//	}
//
//    @Override
//    public void stop() throws Exception {
//
//		// destroy server thread
//		if (thread!=null && thread.isAlive()) {
//			thread.interrupt();
//		}
//
//		// on stop
//		onStoped();
//		logger.info(">>>>>>>>>>> xxl-rpc remoting server destroy success.");
//    }
//
//}
