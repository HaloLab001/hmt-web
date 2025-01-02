/*-------------------------------------------------------------------------
 *
 * ZkServiceRegistry.java
 * ZkServiceRegistry类
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
 *    /hmt-rpc/src/main/java/com/wugui/hmt/rpc/old/registry/impl/ZkServiceRegistry.java
 *
 *-----------------------------------------------
 */
package com.wugui.hmt.rpc.old.registry.impl;//package com.xxl.rpc.registry.impl;
//
//import com.xxl.rpc.registry.ServiceRegistry;
//import com.xxl.rpc.util.XxlRpcException;
//import com.xxl.rpc.util.XxlZkClient;
//import org.apache.zookeeper.WatchedEvent;
//import org.apache.zookeeper.Watcher;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.concurrent.ConcurrentMap;
//import java.util.concurrent.TimeUnit;
//
///**
// * service registry for "zookeeper"
// *
// *  /xxl-rpc/dev/
// *              - key01(service01)
// *                  - value01 (ip:port01)
// *                  - value02 (ip:port02)
// *
// *
// *
// *        <!-- zookeeper (provided) -->
// *        <dependency>
// *            <groupId>org.apache.zookeeper</groupId>
// *            <artifactId>zookeeper</artifactId>
// *            <version>${zookeeper.version}</version>
// *            <scope>provided</scope>
// *        </dependency>
// *
// *
// * @author xuxueli 2018-10-17
// */
//public class ZkServiceRegistry extends ServiceRegistry {
//    private static Logger logger = LoggerFactory.getLogger(ZkServiceRegistry.class);
//
//    // param
//    public static final String ENV = "env";                       // zk env
//    public static final String ZK_ADDRESS = "zkaddress";        // zk registry address, like "ip1:port,ip2:port,ip3:port"
//    public static final String ZK_DIGEST = "zkdigest";          // zk registry digest
//
//
//    // ------------------------------ zk conf ------------------------------
//
//    // config
//    private static final String zkBasePath = "/xxl-rpc";
//    private String zkEnvPath;
//    private XxlZkClient xxlZkClient = null;
//
//    private Thread refreshThread;
//    private volatile boolean refreshThreadStop = false;
//
//    private volatile ConcurrentMap<String, TreeSet<String>> registryData = new ConcurrentHashMap<String, TreeSet<String>>();
//    private volatile ConcurrentMap<String, TreeSet<String>> discoveryData = new ConcurrentHashMap<String, TreeSet<String>>();
//
//
//    /**
//     * key 2 path
//     * @param   nodeKey
//     * @return  znodePath
//     */
//    public String keyToPath(String nodeKey){
//        return zkEnvPath + "/" + nodeKey;
//    }
//
//    /**
//     * path 2 key
//     * @param   nodePath
//     * @return  nodeKey
//     */
//    public String pathToKey(String nodePath){
//        if (nodePath==null || nodePath.length() <= zkEnvPath.length() || !nodePath.startsWith(zkEnvPath)) {
//            return null;
//        }
//        return nodePath.substring(zkEnvPath.length()+1, nodePath.length());
//    }
//
//    // ------------------------------ util ------------------------------
//
//    /**
//     * @param param
//     *      Environment.ZK_ADDRESS  ：zk address
//     *      Environment.ZK_DIGEST   ：zk didest
//     *      Environment.ENV         ：env
//     */
//    @Override
//    public void start(Map<String, String> param) {
//        String zkaddress = param.get(ZK_ADDRESS);
//        String zkdigest = param.get(ZK_DIGEST);
//        String env = param.get(ENV);
//
//        // valid
//        if (zkaddress==null || zkaddress.trim().length()==0) {
//            throw new XxlRpcException("xxl-rpc zkaddress can not be empty");
//        }
//
//        // init zkpath
//        if (env==null || env.trim().length()==0) {
//            throw new XxlRpcException("xxl-rpc env can not be empty");
//        }
//
//        zkEnvPath = zkBasePath.concat("/").concat(env);
//
//        // init
//        xxlZkClient = new XxlZkClient(zkaddress, zkEnvPath, zkdigest, new Watcher() {
//            @Override
//            public void process(WatchedEvent watchedEvent) {
//                try {
//                    logger.debug(">>>>>>>>>>> xxl-rpc: watcher:{}", watchedEvent);
//
//                    // session expire, close old and create new
//                    if (watchedEvent.getState() == Event.KeeperState.Expired) {
//                        xxlZkClient.destroy();
//                        xxlZkClient.getClient();
//
//                        // refreshDiscoveryData (all)：expire retry
//                        refreshDiscoveryData(null);
//
//                        logger.info(">>>>>>>>>>> xxl-rpc, zk re-connect reloadAll success.");
//                    }
//
//                    // watch + refresh
//                    String path = watchedEvent.getPath();
//                    String key = pathToKey(path);
//                    if (key != null) {
//                        // keep watch conf key：add One-time trigger
//                        xxlZkClient.getClient().exists(path, true);
//
//                        // refresh
//                        if (watchedEvent.getType() == Event.EventType.NodeChildrenChanged) {
//                            // refreshDiscoveryData (one)：one change
//                            refreshDiscoveryData(key);
//                        } else if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
//                            logger.info("reload all 111");
//                        }
//                    }
//
//                } catch (Exception e) {
//                    logger.error(e.getMessage(), e);
//                }
//            }
//        });
//
//        // init client      // TODO, support init without conn, and can use mirror data
//        xxlZkClient.getClient();
//
//
//        // refresh thread
//        refreshThread = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                while (!refreshThreadStop) {
//                    try {
//                        TimeUnit.SECONDS.sleep(60);
//
//                        // refreshDiscoveryData (all)：cycle check
//                        refreshDiscoveryData(null);
//
//                        // refresh RegistryData
//                        refreshRegistryData();
//                    } catch (Exception e) {
//                        if (!refreshThreadStop) {
//                            logger.error(">>>>>>>>>>> xxl-rpc, refresh thread error.", e);
//                        }
//                    }
//                }
//                logger.info(">>>>>>>>>>> xxl-rpc, refresh thread stoped.");
//            }
//        });
//        refreshThread.setName("xxl-rpc, ZkServiceRegistry refresh thread.");
//        refreshThread.setDaemon(true);
//        refreshThread.start();
//
//        logger.info(">>>>>>>>>>> xxl-rpc, ZkServiceRegistry init success. [env={}]", env);
//    }
//
//    @Override
//    public void stop() {
//        if (xxlZkClient!=null) {
//            xxlZkClient.destroy();
//        }
//        if (refreshThread != null) {
//            refreshThreadStop = true;
//            refreshThread.interrupt();
//        }
//    }
//
//    /**
//     * refresh discovery data, and cache
//     *
//     * @param key
//     */
//    private void refreshDiscoveryData(String key){
//
//        Set<String> keys = new HashSet<String>();
//        if (key!=null && key.trim().length()>0) {
//            keys.add(key);
//        } else {
//            if (discoveryData.size() > 0) {
//                keys.addAll(discoveryData.keySet());
//            }
//        }
//
//        if (keys.size() > 0) {
//            for (String keyItem: keys) {
//
//                // add-values
//                String path = keyToPath(keyItem);
//                Map<String, String> childPathData = xxlZkClient.getChildPathData(path);
//
//                // exist-values
//                TreeSet<String> existValues = discoveryData.get(keyItem);
//                if (existValues == null) {
//                    existValues = new TreeSet<String>();
//                    discoveryData.put(keyItem, existValues);
//                }
//
//                if (childPathData.size() > 0) {
//                	existValues.clear();
//                    existValues.addAll(childPathData.keySet());
//                }
//            }
//            logger.info(">>>>>>>>>>> xxl-rpc, refresh discovery data success, discoveryData = {}", discoveryData);
//        }
//    }
//
//    /**
//     * refresh registry data
//     */
//    private void refreshRegistryData(){
//        if (registryData.size() > 0) {
//            for (Map.Entry<String, TreeSet<String>> item: registryData.entrySet()) {
//                String key = item.getKey();
//                for (String value:item.getValue()) {
//                    // make path, child path
//                    String path = keyToPath(key);
//                    xxlZkClient.setChildPathData(path, value, "");
//                }
//            }
//            logger.info(">>>>>>>>>>> xxl-rpc, refresh registry data success, registryData = {}", registryData);
//        }
//    }
//
//    @Override
//    public boolean registry(Set<String> keys, String value) {
//        for (String key : keys) {
//            // local cache
//            TreeSet<String> values = registryData.get(key);
//            if (values == null) {
//                values = new TreeSet<>();
//                registryData.put(key, values);
//            }
//            values.add(value);
//
//            // make path, child path
//            String path = keyToPath(key);
//            xxlZkClient.setChildPathData(path, value, "");
//        }
//        logger.info(">>>>>>>>>>> xxl-rpc, registry success, keys = {}, value = {}", keys, value);
//        return true;
//    }
//
//    @Override
//    public boolean remove(Set<String> keys, String value) {
//        for (String key : keys) {
//            TreeSet<String> values = discoveryData.get(key);
//            if (values != null) {
//                values.remove(value);
//            }
//            String path = keyToPath(key);
//            xxlZkClient.deleteChildPath(path, value);
//        }
//        logger.info(">>>>>>>>>>> xxl-rpc, remove success, keys = {}, value = {}", keys, value);
//        return true;
//    }
//
//    @Override
//    public Map<String, TreeSet<String>> discovery(Set<String> keys) {
//        if (keys==null || keys.size()==0) {
//            return null;
//        }
//        Map<String, TreeSet<String>> registryDataTmp = new HashMap<String, TreeSet<String>>();
//        for (String key : keys) {
//            TreeSet<String> valueSetTmp = discovery(key);
//            if (valueSetTmp != null) {
//                registryDataTmp.put(key, valueSetTmp);
//            }
//        }
//        return registryDataTmp;
//    }
//
//    @Override
//    public TreeSet<String> discovery(String key) {
//
//        // local cache
//        TreeSet<String> values = discoveryData.get(key);
//        if (values == null) {
//
//            // refreshDiscoveryData (one)：first use
//            refreshDiscoveryData(key);
//
//            values = discoveryData.get(key);
//        }
//
//        return values;
//    }
//
//}
