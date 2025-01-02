package com.wugui.hmt.admin.controller;

import com.wugui.hmt.core.biz.ExecutorBiz;
import com.wugui.hmt.core.biz.model.LogResult;
import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.core.glue.GlueTypeEnum;
import com.wugui.hmt.core.util.DateUtil;
import com.wugui.hmt.admin.core.kill.KillJob;
import com.wugui.hmt.admin.core.scheduler.JobScheduler;
import com.wugui.hmt.admin.core.util.I18nUtil;
import com.wugui.hmt.admin.entity.JobInfo;
import com.wugui.hmt.admin.entity.JobLog;
import com.wugui.hmt.admin.mapper.JobInfoMapper;
import com.wugui.hmt.admin.mapper.JobLogMapper;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/*-------------------------------------------------------------------------
 *
 * JobLogController.java
 *   任务运行日志相关接口
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
 *	  hmt-admin/src/main/java/com/wugui/hmt/admin/controller/JobLogController.java
 *
 *-------------------------------------------------------------------------
 */
@RestController
@RequestMapping("/api/log")
@Api(tags = "任务运行日志接口")
public class JobLogController {
    private static Logger logger = LoggerFactory.getLogger(JobLogController.class);

    @Resource
    public JobInfoMapper jobInfoMapper;
    @Resource
    public JobLogMapper jobLogMapper;

    @GetMapping("/pageList")
    @ApiOperation("运行日志列表")
    public ReturnT<Map<String, Object>> pageList(
            @RequestParam(required = false, defaultValue = "0") int current,
            @RequestParam(required = false, defaultValue = "10") int size,
            int jobGroup, int jobId, int logStatus, String filterTime,
            String jobDesc, String metaType) {

        // valid permission
        //JobInfoController.validPermission(request, jobGroup);	// 仅管理员支持查询全部；普通用户仅支持查询有权限的 jobGroup

        // parse param
        Date triggerTimeStart = null;
        Date triggerTimeEnd = null;
        if (filterTime != null && filterTime.trim().length() > 0) {
            String[] temp = filterTime.split(" - ");
            if (temp.length == 2) {
                triggerTimeStart = DateUtil.parseDateTime(temp[0]);
                triggerTimeEnd = DateUtil.parseDateTime(temp[1]);
            }
        }

        // page query
        List<JobLog> data = jobLogMapper.pageList((current - 1) * size, size, jobGroup, jobId, triggerTimeStart, triggerTimeEnd, logStatus, jobDesc, metaType);
        int cnt = jobLogMapper.pageListCount((current - 1) * size, size, jobGroup, jobId, triggerTimeStart, triggerTimeEnd, logStatus, jobDesc, metaType);

        // package result
        Map<String, Object> maps = new HashMap<>();
        maps.put("recordsTotal", cnt);        // 总记录数
        maps.put("recordsFiltered", cnt);    // 过滤后的总记录数
        maps.put("data", data);                    // 分页列表
        return new ReturnT<>(maps);
    }

    @RequestMapping(value = "/logDetailCat", method = RequestMethod.GET)
    @ApiOperation("运行日志详情")
    public ReturnT<LogResult> logDetailCat(String executorAddress, long triggerTime, long logId, int fromLineNum) {
        try {
            ExecutorBiz executorBiz = JobScheduler.getExecutorBiz(executorAddress);
            ReturnT<LogResult> logResult = executorBiz.log(triggerTime, logId, fromLineNum);

            // is end
            if (logResult.getContent() != null && fromLineNum > logResult.getContent().getToLineNum()) {
                JobLog jobLog = jobLogMapper.load(logId);
                if (jobLog.getHandleCode() > 0) {
                    logResult.getContent().setEnd(true);
                }
            }

            return logResult;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return new ReturnT<>(ReturnT.FAIL_CODE, e.getMessage());
        }
    }

    @RequestMapping(value = "/logKill", method = RequestMethod.POST)
    @ApiOperation("kill任务")
    public ReturnT<String> logKill(long id) {
        // base check
        JobLog log = jobLogMapper.load(id);
        JobInfo jobInfo = jobInfoMapper.loadById(log.getJobId());
        if (jobInfo == null) {
            return new ReturnT<>(500, I18nUtil.getString("jobinfo_glue_jobid_invalid"));
        }
        if (ReturnT.SUCCESS_CODE != log.getTriggerCode()) {
            return new ReturnT<>(500, I18nUtil.getString("joblog_kill_log_limit"));
        }

        // request of kill
        ReturnT<String> runResult;
        try {
            ExecutorBiz executorBiz = JobScheduler.getExecutorBiz(log.getExecutorAddress());
            runResult = executorBiz.kill(jobInfo.getId());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            runResult = new ReturnT<>(500, e.getMessage());
        }

        if (ReturnT.SUCCESS_CODE == runResult.getCode()) {
            log.setHandleCode(ReturnT.FAIL_CODE);
            log.setHandleMsg(I18nUtil.getString("joblog_kill_log_byman") + ":" + (runResult.getMsg() != null ? runResult.getMsg() : ""));
            log.setHandleTime(new Date());
            jobLogMapper.updateHandleInfo(log);
            return new ReturnT<>(runResult.getMsg());
        } else {
            return new ReturnT<>(500, runResult.getMsg());
        }
    }

    @PostMapping("/clearLog")
    @ApiOperation("清理日志")
    public ReturnT<String> clearLog(int jobGroup, int jobId, int type) {

        Date clearBeforeTime = null;
        int clearBeforeNum = 0;
        if (type == 1) {
            clearBeforeTime = DateUtil.addMonths(new Date(), -1);    // 清理一个月之前日志数据
        } else if (type == 2) {
            clearBeforeTime = DateUtil.addMonths(new Date(), -3);    // 清理三个月之前日志数据
        } else if (type == 3) {
            clearBeforeTime = DateUtil.addMonths(new Date(), -6);    // 清理六个月之前日志数据
        } else if (type == 4) {
            clearBeforeTime = DateUtil.addYears(new Date(), -1);    // 清理一年之前日志数据
        } else if (type == 5) {
            clearBeforeNum = 1000;        // 清理一千条以前日志数据
        } else if (type == 6) {
            clearBeforeNum = 10000;        // 清理一万条以前日志数据
        } else if (type == 7) {
            clearBeforeNum = 30000;        // 清理三万条以前日志数据
        } else if (type == 8) {
            clearBeforeNum = 100000;    // 清理十万条以前日志数据
        } else if (type == 9) {
            clearBeforeNum = 0;            // 清理所有日志数据
        } else {
            return new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("joblog_clean_type_invalid"));
        }

        List<Long> logIds;
        do {
            logIds = jobLogMapper.findClearLogIds(jobGroup, jobId, clearBeforeTime, clearBeforeNum, 1000);
            if (logIds != null && logIds.size() > 0) {
                jobLogMapper.clearLog(logIds);
            }
        } while (logIds != null && logIds.size() > 0);

        return ReturnT.SUCCESS;
    }

    @ApiOperation("停止该job作业")
    @PostMapping("/killJob")
    public ReturnT<String> killJob(@RequestBody JobLog log) {
        JobInfo jobInfo = jobInfoMapper.loadById(log.getJobId());
        if (GlueTypeEnum.match(jobInfo.getGlueType()) == GlueTypeEnum.DATAX || GlueTypeEnum.match(jobInfo.getGlueType()).isScript()) {
            return KillJob.trigger(log.getId(), log.getTriggerTime(), log.getExecutorAddress(), log.getProcessId());
        } else {
            return this.logKill(log.getId());
        }
    }
}
