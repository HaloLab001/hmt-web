package com.wugui.datax.impl;

import com.wugui.hmt.core.biz.ExecutorBiz;
import com.wugui.hmt.core.biz.model.LogResult;
import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.core.biz.model.TriggerParam;
import com.wugui.hmt.core.enums.ExecutorBlockStrategyEnum;
import com.wugui.hmt.core.executor.JobExecutor;
import com.wugui.hmt.core.glue.GlueTypeEnum;
import com.wugui.hmt.rpc.remoting.invoker.call.CallType;
import com.wugui.hmt.rpc.remoting.invoker.reference.XxlRpcReferenceBean;
import com.wugui.hmt.rpc.remoting.invoker.route.LoadBalance;
import com.wugui.hmt.rpc.remoting.net.impl.netty_http.client.NettyHttpClient;
import com.wugui.hmt.rpc.serialize.impl.HessianSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


/*-------------------------------------------------------------------------
 *
 * ExecutorBizImplTest.java
 * ExecutorBizImplTest类
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
 *    /hmt-core/src/test/java/com/wugui/datax/impl/ExecutorBizImplTest.java
 *
 *-----------------------------------------------
 */
public class ExecutorBizImplTest {

    public JobExecutor jobExecutor = null;
    public ExecutorBiz executorBiz = null;

    @Before
    public void before() throws Exception {

        // init executor
        jobExecutor = new JobExecutor();
        jobExecutor.setAdminAddresses(null);
        jobExecutor.setAppName("datax-executor");
        jobExecutor.setIp(null);
        jobExecutor.setPort(9999);
        jobExecutor.setAccessToken(null);
        jobExecutor.setLogPath("/data/applogs/executor/jobhandler");
        jobExecutor.setLogRetentionDays(-1);

        // start executor
        jobExecutor.start();

        TimeUnit.SECONDS.sleep(3);

        // init executor biz proxy
        XxlRpcReferenceBean referenceBean = new XxlRpcReferenceBean();
        referenceBean.setClient(NettyHttpClient.class);
        referenceBean.setSerializer(HessianSerializer.class);
        referenceBean.setCallType(CallType.SYNC);
        referenceBean.setLoadBalance(LoadBalance.ROUND);
        referenceBean.setIface(ExecutorBiz.class);
        referenceBean.setVersion(null);
        referenceBean.setTimeout(3000);
        referenceBean.setAddress("127.0.0.1:9999");
        referenceBean.setAccessToken(null);
        referenceBean.setInvokeCallback(null);
        referenceBean.setInvokerFactory(null);

        executorBiz = (ExecutorBiz) referenceBean.getObject();
    }

    @After
    public void after(){
        if (jobExecutor != null) {
            jobExecutor.destroy();
        }
    }


    @Test
    public void beat() {
        // Act
        final ReturnT<String> retval = executorBiz.beat();

        // Assert result
        Assert.assertNotNull(retval);
        Assert.assertNull(((ReturnT<String>) retval).getContent());
        Assert.assertEquals(200, retval.getCode());
        Assert.assertNull(retval.getMsg());
    }

    @Test
    public void idleBeat(){
        final int jobId = 0;

        // Act
        final ReturnT<String> retval = executorBiz.idleBeat(jobId);

        // Assert result
        Assert.assertNotNull(retval);
        Assert.assertNull(((ReturnT<String>) retval).getContent());
        Assert.assertEquals(500, retval.getCode());
        Assert.assertEquals("job thread is running or has trigger queue.", retval.getMsg());
    }

    @Test
    public void kill(){
        final int jobId = 0;

        // Act
        final ReturnT<String> retval = executorBiz.kill(jobId);

        // Assert result
        Assert.assertNotNull(retval);
        Assert.assertNull(((ReturnT<String>) retval).getContent());
        Assert.assertEquals(200, retval.getCode());
        Assert.assertNull(retval.getMsg());
    }

    @Test
    public void log(){
        final long logDateTim = 0L;
        final long logId = 0;
        final int fromLineNum = 0;

        // Act
        final ReturnT<LogResult> retval = executorBiz.log(logDateTim, logId, fromLineNum);

        // Assert result
        Assert.assertNotNull(retval);
    }

    @Test
    public void run(){
        // trigger data
        final TriggerParam triggerParam = new TriggerParam();
        triggerParam.setJobId(1);
        triggerParam.setExecutorHandler("demoJobHandler");
        triggerParam.setExecutorParams(null);
        triggerParam.setExecutorBlockStrategy(ExecutorBlockStrategyEnum.COVER_EARLY.name());
        triggerParam.setGlueType(GlueTypeEnum.DATAX.name());
        triggerParam.setGlueSource(null);
        triggerParam.setGlueUpdatetime(System.currentTimeMillis());
        triggerParam.setLogId(1);
        triggerParam.setLogDateTime(System.currentTimeMillis());

        // Act
        final ReturnT<String> retval = executorBiz.run(triggerParam);

        // Assert result
        Assert.assertNotNull(retval);
    }

}
