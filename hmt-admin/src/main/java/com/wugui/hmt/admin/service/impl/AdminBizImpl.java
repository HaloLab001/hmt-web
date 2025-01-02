package com.wugui.hmt.admin.service.impl;

import com.wugui.hmt.core.biz.AdminBiz;
import com.wugui.hmt.core.biz.model.HandleCallbackParam;
import com.wugui.hmt.core.biz.model.HandleProcessCallbackParam;
import com.wugui.hmt.core.biz.model.RegistryParam;
import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.core.enums.IncrementTypeEnum;
import com.wugui.hmt.core.handler.IJobHandler;
import com.wugui.hmt.admin.core.kill.KillJob;
import com.wugui.hmt.admin.core.thread.JobTriggerPoolHelper;
import com.wugui.hmt.admin.core.trigger.TriggerTypeEnum;
import com.wugui.hmt.admin.core.util.I18nUtil;
import com.wugui.hmt.admin.entity.JobInfo;
import com.wugui.hmt.admin.entity.JobLog;
import com.wugui.hmt.admin.mapper.JobInfoMapper;
import com.wugui.hmt.admin.mapper.JobLogMapper;
import com.wugui.hmt.admin.mapper.JobRegistryMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.text.MessageFormat;
import java.util.Date;
import java.util.List;

/*-------------------------------------------------------------------------
 *
 * SubMetaDataMapper.java
 *   
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
 * This software and related documentation are provided under a 
 * license agreement containing restrictions on use and disclosure
 * and are protected by intellectual property laws. Except as expressly
 * permitted in your license agreement or allowed by law, you may not 
 * use, copy, reproduce, translate, broadcast, modify, license, transmit,
 * distribute, exhibit, perform, publish, or display any part, in any 
 * form, or by any means. Reverse engineering, disassembly, or 
 * decompilation of this software, unless required by law for 
 * interoperability, is prohibited.
 * 
 * This software is developed for general use in a variety of
 * information management applications. It is not developed or intended
 * for use in any inherently dangerous applications, including 
 * applications that may create a risk of personal injury. If you use 
 * this software or in dangerous applications, then you shall be 
 * responsible to take all appropriate fail-safe, backup, redundancy,
 * and other measures to ensure its safe use. Halo Tech Corporation and
 * its affiliates disclaim any liability for any damages caused by use
 * of this software in dangerous applications.
 * 
 *
 * IDENTIFICATION
 *	  hmt-admin/src/main/java/com/wugui/hmt/admin/mapper/SubMetaDataMapper.java
 *
 *-------------------------------------------------------------------------
 */
@Service
public class AdminBizImpl implements AdminBiz {
    private static Logger logger = LoggerFactory.getLogger(AdminBizImpl.class);

    @Resource
    public JobLogMapper jobLogMapper;
    @Resource
    private JobInfoMapper jobInfoMapper;
    @Resource
    private JobRegistryMapper jobRegistryMapper;

    @Override
    public ReturnT<String> callback(List<HandleCallbackParam> callbackParamList) {
        for (HandleCallbackParam handleCallbackParam : callbackParamList) {
            ReturnT<String> callbackResult = callback(handleCallbackParam);
            logger.debug(">>>>>>>>> JobApiController.callback {}, handleCallbackParam={}, callbackResult={}",
                    (callbackResult.getCode() == IJobHandler.SUCCESS.getCode() ? "success" : "fail"), handleCallbackParam, callbackResult);
        }

        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> processCallback(List<HandleProcessCallbackParam> callbackParamList) {
        for (HandleProcessCallbackParam handleProcessCallbackParam : callbackParamList) {
            ReturnT<String> callbackResult = processCallback(handleProcessCallbackParam);
            logger.debug(">>>>>>>>> JobApiController.processCallback {}, handleCallbackParam={}, callbackResult={}",
                    (callbackResult.getCode() == IJobHandler.SUCCESS.getCode() ? "success" : "fail"), handleProcessCallbackParam, callbackResult);
        }
        return ReturnT.SUCCESS;
    }

    private ReturnT<String> processCallback(HandleProcessCallbackParam handleProcessCallbackParam) {
        int result = jobLogMapper.updateProcessId(handleProcessCallbackParam.getLogId(), handleProcessCallbackParam.getProcessId());
        return result > 0 ? ReturnT.FAIL : ReturnT.SUCCESS;
    }


    private ReturnT<String> callback(HandleCallbackParam handleCallbackParam) {
        // valid log item
        JobLog log = jobLogMapper.load(handleCallbackParam.getLogId());
        if (log == null) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "log item not found.");
        }
        if (log.getHandleCode() > 0) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "log repeate callback.");     // avoid repeat callback, trigger child job etc
        }

        // trigger success, to trigger child job
        String callbackMsg = null;
        int resultCode = handleCallbackParam.getExecuteResult().getCode();

        if (IJobHandler.SUCCESS.getCode() == resultCode) {

            JobInfo jobInfo = jobInfoMapper.loadById(log.getJobId());

            updateIncrementParam(log, jobInfo.getIncrementType());

            if (jobInfo != null && jobInfo.getChildJobId() != null && jobInfo.getChildJobId().trim().length() > 0) {
                callbackMsg = "<br><br><span style=\"color:#00c0ef;\" > >>>>>>>>>>>" + I18nUtil.getString("jobconf_trigger_child_run") + "<<<<<<<<<<< </span><br>";

                String[] childJobIds = jobInfo.getChildJobId().split(",");
                for (int i = 0; i < childJobIds.length; i++) {
                    int childJobId = (childJobIds[i] != null && childJobIds[i].trim().length() > 0 && isNumeric(childJobIds[i])) ? Integer.valueOf(childJobIds[i]) : -1;
                    if (childJobId > 0) {

                        JobTriggerPoolHelper.trigger(childJobId, TriggerTypeEnum.PARENT, -1, null, null);
                        ReturnT<String> triggerChildResult = ReturnT.SUCCESS;

                        // add msg
                        callbackMsg += MessageFormat.format(I18nUtil.getString("jobconf_callback_child_msg1"),
                                (i + 1),
                                childJobIds.length,
                                childJobIds[i],
                                (triggerChildResult.getCode() == ReturnT.SUCCESS_CODE ? I18nUtil.getString("system_success") : I18nUtil.getString("system_fail")),
                                triggerChildResult.getMsg());
                    } else {
                        callbackMsg += MessageFormat.format(I18nUtil.getString("jobconf_callback_child_msg2"),
                                (i + 1),
                                childJobIds.length,
                                childJobIds[i]);
                    }
                }

            }
        }

        //kill execution timeout DataX process
        if (!StringUtils.isEmpty(log.getProcessId()) && IJobHandler.FAIL_TIMEOUT.getCode() == resultCode) {
            KillJob.trigger(log.getId(), log.getTriggerTime(), log.getExecutorAddress(), log.getProcessId());
        }

        // handle msg
        StringBuffer handleMsg = new StringBuffer();
        if (log.getHandleMsg() != null) {
            handleMsg.append(log.getHandleMsg()).append("<br>");
        }
        if (handleCallbackParam.getExecuteResult().getMsg() != null) {
            handleMsg.append(handleCallbackParam.getExecuteResult().getMsg());
        }
        if (callbackMsg != null) {
            handleMsg.append(callbackMsg);
        }

        if (handleMsg.length() > 15000) {
            handleMsg = new StringBuffer(handleMsg.substring(0, 15000));  // text最大64kb 避免长度过长
        }

        // success, save log
        log.setHandleTime(new Date());
        log.setHandleCode(resultCode);
        log.setHandleMsg(handleMsg.toString());
        jobLogMapper.updateHandleInfo(log);
        jobInfoMapper.updateLastHandleCode(log.getJobId(), resultCode);
        return ReturnT.SUCCESS;
    }

    private void updateIncrementParam(JobLog log, Integer incrementType) {
        if (IncrementTypeEnum.ID.getCode() == incrementType) {
            jobInfoMapper.incrementIdUpdate(log.getJobId(),log.getMaxId());
        } else if (IncrementTypeEnum.TIME.getCode() == incrementType) {
            jobInfoMapper.incrementTimeUpdate(log.getJobId(), log.getTriggerTime());
        }
    }

    private boolean isNumeric(String str) {
        try {
            Integer.valueOf(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    public ReturnT<String> registry(RegistryParam registryParam) {

        // valid
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }

        int ret = jobRegistryMapper.registryUpdate(registryParam.getRegistryGroup(), registryParam.getRegistryKey(),
                registryParam.getRegistryValue(), registryParam.getCpuUsage(), registryParam.getMemoryUsage(), registryParam.getLoadAverage(), new Date());
        if (ret < 1) {
            jobRegistryMapper.registrySave(registryParam.getRegistryGroup(), registryParam.getRegistryKey(),
                    registryParam.getRegistryValue(), registryParam.getCpuUsage(), registryParam.getMemoryUsage(), registryParam.getLoadAverage(), new Date());

            // fresh
            freshGroupRegistryInfo(registryParam);
        }
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> registryRemove(RegistryParam registryParam) {

        // valid
        if (!StringUtils.hasText(registryParam.getRegistryGroup())
                || !StringUtils.hasText(registryParam.getRegistryKey())
                || !StringUtils.hasText(registryParam.getRegistryValue())) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, "Illegal Argument.");
        }

        int ret = jobRegistryMapper.registryDelete(registryParam.getRegistryGroup(), registryParam.getRegistryKey(), registryParam.getRegistryValue());
        if (ret > 0) {

            // fresh
            freshGroupRegistryInfo(registryParam);
        }
        return ReturnT.SUCCESS;
    }

    private void freshGroupRegistryInfo(RegistryParam registryParam) {
        // Under consideration, prevent affecting core tables
    }

}
