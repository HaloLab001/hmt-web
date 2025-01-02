package com.wugui.hmt.admin.controller;


import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.core.util.DateUtil;
import com.wugui.hmt.admin.core.cron.CronExpression;
import com.wugui.hmt.admin.core.thread.JobTriggerPoolHelper;
import com.wugui.hmt.admin.core.trigger.TriggerTypeEnum;
import com.wugui.hmt.admin.core.util.I18nUtil;
import com.wugui.hmt.admin.dto.DataXBatchJsonBuildDto;
import com.wugui.hmt.admin.dto.TriggerJobDto;
import com.wugui.hmt.admin.entity.JobInfo;
import com.wugui.hmt.admin.service.JobService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;


/*-------------------------------------------------------------------------
 *
 * JobInfoController.java
 *   任务配置相关的接口
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
 *	  hmt-admin/src/main/java/com/wugui/hmt/admin/controller/JobInfoController.java
 *
 *-------------------------------------------------------------------------
 */
@Api(tags = "任务配置接口")
@RestController
@RequestMapping("/api/job")
public class JobInfoController extends BaseController{

    @Resource
    private JobService jobService;


    @GetMapping("/pageList")
    @ApiOperation("任务列表")
    public ReturnT<Map<String, Object>> pageList(@RequestParam(required = false, defaultValue = "0") int current,
                                        @RequestParam(required = false, defaultValue = "10") int size,
                                        int jobGroup, int triggerStatus, String jobDesc, String glueType, Integer[] projectIds,
                                        String handleCode, String metaType, String tableText, int isBigType) {

        return new ReturnT<>(jobService.pageList((current-1)*size, size, jobGroup, triggerStatus, jobDesc, glueType, 0, projectIds, handleCode, metaType, tableText, isBigType));
    }

    @GetMapping("/list")
    @ApiOperation("全部任务列表")
    public ReturnT<List<JobInfo>> list(){
        return new ReturnT<>(jobService.list());
    }

    @PostMapping("/add")
    @ApiOperation("添加任务")
    public ReturnT<String> add(HttpServletRequest request, @RequestBody JobInfo jobInfo) {
        jobInfo.setUserId(getCurrentUserId(request));
        return jobService.add(jobInfo);
    }

    @PostMapping("/update")
    @ApiOperation("更新任务")
    public ReturnT<String> update(HttpServletRequest request,@RequestBody JobInfo jobInfo) {
        jobInfo.setUserId(getCurrentUserId(request));
        return jobService.update(jobInfo);
    }

    @PostMapping(value = "/remove/{id}")
    @ApiOperation("移除任务")
    public ReturnT<String> remove(@PathVariable(value = "id") int id) {
        return jobService.remove(id);
    }

    @RequestMapping(value = "/stop",method = RequestMethod.POST)
    @ApiOperation("停止任务")
    public ReturnT<String> pause(int id) {
        return jobService.stop(id);
    }

    @RequestMapping(value = "/start",method = RequestMethod.POST)
    @ApiOperation("开启任务")
    public ReturnT<String> start(int id) {
//        return jobService.start(id);
        // JobTriggerPoolHelper.trigger(id, TriggerTypeEnum.MANUAL, -1, null, "");

        return jobService.singleStart(id);
    }

    @RequestMapping(value = "/batchStart",method = RequestMethod.POST)
    @ApiOperation("开启任务")
    public ReturnT<String> batchStart(String jobDesc, Integer[] projectIds, String glueType, String metaType, String tableText, int isBigType) {
        return jobService.batchStart(jobDesc, projectIds, glueType, metaType, tableText, isBigType);
    }

    @PostMapping(value = "/trigger")
    @ApiOperation("触发任务")
    public ReturnT<String> triggerJob(@RequestBody TriggerJobDto dto) {
        // force cover job param
        // String executorParam=dto.getExecutorParam();
        // if (executorParam == null) {
        //     executorParam = "";
        // }
        // JobTriggerPoolHelper.trigger(dto.getJobId(), TriggerTypeEnum.MANUAL, -1, null, executorParam);
        // return ReturnT.SUCCESS;
        return jobService.singleStart(dto.getJobId());
    }

    @GetMapping("/nextTriggerTime")
    @ApiOperation("获取近5次触发时间")
    public ReturnT<List<String>> nextTriggerTime(String cron) {
        List<String> result = new ArrayList<>();
        try {
            CronExpression cronExpression = new CronExpression(cron);
            Date lastTime = new Date();
            for (int i = 0; i < 5; i++) {
                lastTime = cronExpression.getNextValidTimeAfter(lastTime);
                if (lastTime != null) {
                    result.add(DateUtil.formatDateTime(lastTime));
                } else {
                    break;
                }
            }
        } catch (ParseException e) {
            return new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("jobinfo_field_cron_invalid"));
        }
        return new ReturnT<>(result);
    }

    @PostMapping("/batchAdd")
    @ApiOperation("批量创建任务")
    public ReturnT<String> batchAdd(@RequestBody DataXBatchJsonBuildDto dto) throws IOException {
        if (dto.getTemplateId() ==0) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_choose") + I18nUtil.getString("jobinfo_field_temp")));
        }
        return jobService.batchAdd(dto);
    }

    @RequestMapping(value = "/batchStartFailTask",method = RequestMethod.POST)
    @ApiOperation("批量开启失败任务")
    public ReturnT<String> batchStartFailTask(String jobDesc, Integer[] projectIds, String glueType, String metaType, String tableText, int isBigType) {
        return jobService.batchStartFailTask(jobDesc, projectIds, glueType, metaType, tableText, isBigType);
    }

    @RequestMapping(value = "/batchDeleteTask",method = RequestMethod.POST)
    @ApiOperation("批量删除任务")
    public ReturnT<String> batchDeleteTask(String jobDesc, Integer[] projectIds, String glueType, String handleCode, String metaType, String tableText, int isBigType) {
        return jobService.batchDeleteTask(jobDesc, projectIds, glueType, handleCode, metaType, tableText, isBigType);
    }

    @RequestMapping(value = "/batchStopTask",method = RequestMethod.POST)
    @ApiOperation("批量停止任务")
    public ReturnT<String> batchStopTask(String jobDesc, Integer[] projectIds, String glueType, String metaType, String tableText, int isBigType) {
        return jobService.batchStopTask(jobDesc, projectIds, glueType, metaType, tableText, isBigType);
    }

    @GetMapping("/batchSetDataWay")
    @ApiOperation("批量设置数据写入方式")
    public ReturnT<String> batchSetDataWay(int jobGroup, int triggerStatus, String jobDesc, String glueType, Integer[] projectIds,
                                           String handleCode, String metaType, String tableText, int isBigType, String writerType) {
        return jobService.batchSetDataWay(jobGroup, triggerStatus, jobDesc, glueType, projectIds, handleCode, metaType, tableText, isBigType, writerType);
    }

}
