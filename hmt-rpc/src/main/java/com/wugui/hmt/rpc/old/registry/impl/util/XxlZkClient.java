/*-------------------------------------------------------------------------
 *
 * XxlZkClient.java
 * XxlZkClient类
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
 *    /hmt-rpc/src/main/java/com/wugui/hmt/rpc/old/registry/impl/util/XxlZkClient.java
 *
 *-----------------------------------------------
 */
package com.wugui.hmt.rpc.old.registry.impl.util;//package com.xxl.rpc.util;
//
//import org.apache.zookeeper.*;
//import org.apache.zookeeper.data.Stat;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.locks.ReentrantLock;
//
//
///**
// * ZooKeeper cfg client (Watcher + some utils)
// *
// * @author xuxueli 2015-08-26 21:36:43
// */
//public class XxlZkClient {
//	private static Logger logger = LoggerFactory.getLogger(XxlZkClient.class);
//
//
//	private String zkaddress;
//	private String zkpath;
//	private String zkdigest;
//	private Watcher watcher;	// watcher(One-time trigger)
//
//
//	public XxlZkClient(String zkaddress, String zkpath, String zkdigest, Watcher watcher) {
//
//		this.zkaddress = zkaddress;
//		this.zkpath = zkpath;
//		this.zkdigest = zkdigest;
//		this.watcher = watcher;
//
//		// reconnect when expire
//		if (this.watcher == null) {
//			// watcher(One-time trigger)
//			this.watcher = new Watcher() {
//				@Override
//				public void process(WatchedEvent watchedEvent) {
//					logger.info(">>>>>>>>>>> xxl-rpc: watcher:{}", watchedEvent);
//
//					// session expire, close old and create new
//					if (watchedEvent.getState() == Event.KeeperState.Expired) {
//						destroy();
//						getClient();
//					}
//				}
//			};
//		}
//
//		//getClient();		// async coon, support init without conn
//	}
//
//	// ------------------------------ zookeeper client ------------------------------
//	private ZooKeeper zooKeeper;
//	private ReentrantLock INSTANCE_INIT_LOCK = new ReentrantLock(true);
//	public ZooKeeper getClient(){
//		if (zooKeeper==null) {
//			try {
//				if (INSTANCE_INIT_LOCK.tryLock(2, TimeUnit.SECONDS)) {
//
//                    // init new-client
//                    ZooKeeper newZk = null;
//                    try {
//                        if (zooKeeper == null) {		// 二次校验，防止并发创建client
//                            newZk = new ZooKeeper(zkaddress, 10000, watcher);
//                            if (zkdigest!=null && zkdigest.trim().length()>0) {
//                                newZk.addAuthInfo("digest",zkdigest.getBytes());		// like "account:password"
//                            }
//                            newZk.exists(zkpath, false);		// sync wait until succcess conn
//
//                            // set success new-client
//                            zooKeeper = newZk;
//                            logger.info(">>>>>>>>>>> xxl-rpc, XxlZkClient init success.");
//                        }
//                    } catch (Exception e) {
//                        // close fail new-client
//                        if (newZk != null) {
//                            newZk.close();
//                        }
//
//                        logger.error(e.getMessage(), e);
//                    } finally {
//                        INSTANCE_INIT_LOCK.unlock();
//                    }
//
//				}
//			} catch (Exception e) {
//				logger.error(e.getMessage(), e);
//			}
//		}
//		if (zooKeeper == null) {
//			throw new XxlRpcException("XxlZkClient.zooKeeper is null.");
//		}
//		return zooKeeper;
//	}
//
//	public void destroy(){
//		if (zooKeeper!=null) {
//			try {
//				zooKeeper.close();
//				zooKeeper = null;
//			} catch (Exception e) {
//				logger.error(e.getMessage(), e);
//			}
//		}
//	}
//
//	// ------------------------------ util ------------------------------
//
//	/**
//	 * create node path with parent path (PERSISTENT)
//	 *
//	 * zk limit parent must exist
//	 *
//	 * @param path
//	 * @param watch
//	 */
//	private Stat createPathWithParent(String path, boolean watch){
//		// valid
//		if (path==null || path.trim().length()==0) {
//			return null;
//		}
//
//		try {
//			Stat stat = getClient().exists(path, watch);
//			if (stat == null) {
//				//  valid parent, createWithParent if not exists
//				if (path.lastIndexOf("/") > 0) {
//					String parentPath = path.substring(0, path.lastIndexOf("/"));
//					Stat parentStat = getClient().exists(parentPath, watch);
//					if (parentStat == null) {
//						createPathWithParent(parentPath, false);
//					}
//				}
//				// create desc node path
//				getClient().create(path, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//			}
//			return getClient().exists(path, true);
//		} catch (Exception e) {
//			throw new XxlRpcException(e);
//		}
//	}
//
//	/**
//	 * delete path (watch)
//	 *
//	 * @param path
//	 * @param watch
//	 */
//	public void deletePath(String path, boolean watch){
//		try {
//			Stat stat = getClient().exists(path, watch);
//			if (stat != null) {
//				getClient().delete(path, stat.getVersion());
//			} else {
//				logger.info(">>>>>>>>>>> zookeeper node path not found :{}", path);
//			}
//		} catch (Exception e) {
//			throw new XxlRpcException(e);
//		}
//	}
//
//	/**
//	 * set data to node (watch)
//	 * @param path
//	 * @param data
//	 * @param watch
//	 * @return
//	 */
//	public Stat setPathData(String path, String data, boolean watch) {
//		try {
//			Stat stat = getClient().exists(path, watch);
//			if (stat == null) {
//				createPathWithParent(path, watch);
//				stat = getClient().exists(path, watch);
//			}
//			return getClient().setData(path, data.getBytes("UTF-8"), stat.getVersion());
//		} catch (Exception e) {
//			throw new XxlRpcException(e);
//		}
//	}
//
//	/**
//	 * get data from node (watch)
//	 *
//	 * @param path
//	 * @param watch
//	 * @return
//	 */
//	public String getPathData(String path, boolean watch){
//		try {
//			String znodeValue = null;
//			Stat stat = getClient().exists(path, watch);
//			if (stat != null) {
//				byte[] resultData = getClient().getData(path, watch, null);
//				if (resultData != null) {
//					znodeValue = new String(resultData, "UTF-8");
//				}
//			} else {
//				logger.info(">>>>>>>>>>> xxl-rpc, path[{}] not found.", path);
//			}
//			return znodeValue;
//		} catch (Exception e) {
//			throw new XxlRpcException(e);
//		}
//	}
//
//
//	// ---------------------- child ----------------------
//
//	/**
//	 * set child pach data (EPHEMERAL)
//	 *
//	 * @param path
//	 * @param childNode
//	 * @param childNodeData
//	 */
//	public void setChildPathData(String path, String childNode, String childNodeData) {
//		try {
//
//			// set path
//			createPathWithParent(path, false);
//
//
//			// set child path
//			String childNodePath = path.concat("/").concat(childNode);
//
//			Stat stat = getClient().exists(childNodePath, false);
//			if (stat!=null) {	// EphemeralOwner=0、PERSISTENT and delete
//				if (stat.getEphemeralOwner()==0) {
//					getClient().delete(childNodePath, stat.getVersion());
//				} else {
//					return;		// EPHEMERAL and pass
//				}
//			}
//
//			getClient().create(childNodePath, childNodeData.getBytes("UTF-8"), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
//		} catch (Exception e) {
//			throw new XxlRpcException(e);
//		}
//	}
//
//	/**
//	 * delete child path
//	 *
//	 * @param path
//	 * @param childNode
//	 */
//	public void deleteChildPath(String path, String childNode) {
//		try {
//			// delete child path
//			String childNodePath = path.concat("/").concat(childNode);
//			deletePath(childNodePath, false);
//		} catch (Exception e) {
//			throw new XxlRpcException(e);
//		}
//	}
//
//	/**
//	 * get child path data
//	 *
//	 * @return
//	 */
//	public Map<String, String> getChildPathData(String path){
//		Map<String, String> allData = new HashMap<String, String>();
//		try {
//			Stat stat = getClient().exists(path, true);
//			if (stat == null) {
//				return allData;	// no such node
//			}
//
//			List<String> childNodes = getClient().getChildren(path, true);
//			if (childNodes!=null && childNodes.size()>0) {
//				for (String childNode : childNodes) {
//
//					// child data
//					String childNodePath = path.concat("/").concat(childNode);
//					String childNodeValue = getPathData(childNodePath, false);
//
//					allData.put(childNode, childNodeValue);
//				}
//			}
//			return allData;
//		} catch (Exception e) {
//			throw new XxlRpcException(e);
//		}
//	}
//
//
//}
