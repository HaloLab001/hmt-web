package com.wugui.hmt.core.thread;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.wugui.hmt.core.biz.AdminBiz;
import com.wugui.hmt.core.biz.model.HandleProcessCallbackParam;
import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.core.enums.RegistryConfig;
import com.wugui.hmt.core.executor.JobExecutor;
import com.wugui.hmt.core.log.JobFileAppender;
import com.wugui.hmt.core.log.JobLogger;
import com.wugui.hmt.core.util.FileUtil;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/*-------------------------------------------------------------------------
 *
 * ProcessCallbackThread.java
 * ProcessCallbackThread类
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
 *    /hmt-core/src/main/java/com/wugui/datatx/core/thread/ProcessCallbackThread.java
 *
 *-----------------------------------------------
 */
public class ProcessCallbackThread {
    private static Logger logger = LoggerFactory.getLogger(ProcessCallbackThread.class);

    private static ProcessCallbackThread instance = new ProcessCallbackThread();

    public static ProcessCallbackThread getInstance() {
        return instance;
    }

    /**
     * job results callback queue
     */
    private LinkedBlockingQueue<HandleProcessCallbackParam> callBackQueue = new LinkedBlockingQueue<>();

    public static void pushCallBack(HandleProcessCallbackParam callback) {
        getInstance().callBackQueue.add(callback);
        logger.debug(">>>>>>>>>>> hmt-web, push process callback request, logId:{}", callback.getLogId());
    }

    /**
     * callback thread
     */
    private Thread processCallbackThread;
    private Thread processRetryCallbackThread;
    private volatile boolean toStop = false;

    public void start() {

        // valid
        if (JobExecutor.getAdminBizList() == null) {
            logger.warn(">>>>>>>>>>> hmt-web, executor callback config fail, adminAddresses is null.");
            return;
        }

        // callback
        processCallbackThread = new Thread(() -> {

            // normal callback
            while (!toStop) {
                try {
                    HandleProcessCallbackParam callback = getInstance().callBackQueue.take();

                    // callback list param
                    List<HandleProcessCallbackParam> callbackParamList = new ArrayList<HandleProcessCallbackParam>();
                    int drainToNum = getInstance().callBackQueue.drainTo(callbackParamList);
                    callbackParamList.add(callback);

                    // callback, will retry if error
                    if (callbackParamList.size() > 0) {
                        doCallback(callbackParamList);
                    }
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }

            // last callback
            try {
                List<HandleProcessCallbackParam> callbackParamList = new ArrayList<HandleProcessCallbackParam>();
                int drainToNum = getInstance().callBackQueue.drainTo(callbackParamList);
                if (callbackParamList != null && callbackParamList.size() > 0) {
                    doCallback(callbackParamList);
                }
            } catch (Exception e) {
                if (!toStop) {
                    logger.error(e.getMessage(), e);
                }
            }
            logger.info(">>>>>>>>>>> hmt-web, executor callback thread destory.");

        });
        processCallbackThread.setDaemon(true);
        processCallbackThread.setName("hmt-web, executor TriggerCallbackThread");
        processCallbackThread.start();


        // retry
        processRetryCallbackThread = new Thread(() -> {
            while (!toStop) {
                try {
                    retryFailCallbackFile();
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }

                }
                try {
                    TimeUnit.SECONDS.sleep(RegistryConfig.BEAT_TIMEOUT);
                } catch (InterruptedException e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
            logger.info(">>>>>>>>>>> hmt-web, executor retry callback thread destory.");
        });
        processRetryCallbackThread.setDaemon(true);
        processRetryCallbackThread.start();

    }

    public void toStop() {
        toStop = true;
        // stop callback, interrupt and wait
        if (processCallbackThread != null) {    // support empty admin address
            processCallbackThread.interrupt();
            try {
                processCallbackThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

        // stop retry, interrupt and wait
        if (processRetryCallbackThread != null) {
            processRetryCallbackThread.interrupt();
            try {
                processRetryCallbackThread.join();
            } catch (InterruptedException e) {
                logger.error(e.getMessage(), e);
            }
        }

    }

    /**
     * do callback, will retry if error
     *
     * @param callbackParamList
     */
    private void doCallback(List<HandleProcessCallbackParam> callbackParamList) {
        boolean callbackRet = false;
        // callback, will retry if error
        for (AdminBiz adminBiz : JobExecutor.getAdminBizList()) {
            try {
                ReturnT<String> callbackResult = adminBiz.processCallback(callbackParamList);
                if (callbackResult != null && ReturnT.SUCCESS_CODE == callbackResult.getCode()) {
                    callbackLog(callbackParamList, "<br>----------- hmt-web job callback finish.");
                    callbackRet = true;
                    break;
                } else {
                    callbackLog(callbackParamList, "<br>----------- hmt-web job callback fail, callbackResult:" + callbackResult);
                }
            } catch (Exception e) {
                callbackLog(callbackParamList, "<br>----------- hmt-web job callback error, errorMsg:" + e.getMessage());
            }
        }
        if (!callbackRet) {
            appendFailCallbackFile(callbackParamList);
        }
    }

    /**
     * callback log
     */
    private void callbackLog(List<HandleProcessCallbackParam> callbackParamList, String logContent) {
        for (HandleProcessCallbackParam callbackParam : callbackParamList) {
            String logFileName = JobFileAppender.makeLogFileName(new Date(callbackParam.getLogDateTime()), callbackParam.getLogId());
            JobFileAppender.contextHolder.set(logFileName);
            JobLogger.log(logContent);
        }
    }


    // ---------------------- fail-callback file ----------------------

    private static String failCallbackFilePath = JobFileAppender.getLogPath().concat(File.separator).concat("processcallbacklog").concat(File.separator);
    private static String failCallbackFileName = failCallbackFilePath.concat("hmt-web-processcallback-{x}").concat(".log");

    private void appendFailCallbackFile(List<HandleProcessCallbackParam> handleProcessCallbackParams) {
        // valid
        if (handleProcessCallbackParams == null || handleProcessCallbackParams.size() == 0) {
            return;
        }

        // append file
        byte[] callbackParamList_bytes = JobExecutor.getSerializer().serialize(handleProcessCallbackParams);

        File callbackLogFile = new File(failCallbackFileName.replace("{x}", String.valueOf(System.currentTimeMillis())));
        if (callbackLogFile.exists()) {
            for (int i = 0; i < 100; i++) {
                callbackLogFile = new File(failCallbackFileName.replace("{x}", String.valueOf(System.currentTimeMillis()).concat("-").concat(String.valueOf(i))));
                if (!callbackLogFile.exists()) {
                    break;
                }
            }
        }
        FileUtil.writeFileContent(callbackLogFile, callbackParamList_bytes);
    }

    private void retryFailCallbackFile() {

        // valid
        File callbackLogPath = new File(failCallbackFilePath);
        if (!callbackLogPath.exists()) {
            return;
        }
        if (callbackLogPath.isFile()) {
            callbackLogPath.delete();
        }
        if (!(callbackLogPath.isDirectory() && callbackLogPath.list() != null && callbackLogPath.list().length > 0)) {
            return;
        }

        // load and clear file, retry
        List<HandleProcessCallbackParam> params;
        for (File f : callbackLogPath.listFiles()) {
            byte[] ps = FileUtil.readFileContent(f);
            params = (List<HandleProcessCallbackParam>) JobExecutor.getSerializer().deserialize(ps, HandleProcessCallbackParam.class);
            f.delete();
            doCallback(params);
        }
    }

}
