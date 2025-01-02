package com.wugui.hmt.admin.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.core.enums.ExecutorBlockStrategyEnum;
import com.wugui.hmt.core.glue.GlueTypeEnum;
import com.wugui.hmt.core.util.DateUtil;
import com.wugui.hmt.admin.core.conf.JobAdminConfig;
import com.wugui.hmt.admin.core.cron.CronExpression;
import com.wugui.hmt.admin.core.route.ExecutorRouteStrategyEnum;
import com.wugui.hmt.admin.core.thread.JobScheduleHelper;
import com.wugui.hmt.admin.core.thread.JobTriggerPoolHelper;
import com.wugui.hmt.admin.core.trigger.TriggerTypeEnum;
import com.wugui.hmt.admin.core.util.I18nUtil;
import com.wugui.hmt.admin.dto.DataXBatchJsonBuildDto;
import com.wugui.hmt.admin.dto.DataXJsonBuildDto;
import com.wugui.hmt.admin.entity.JobGroup;
import com.wugui.hmt.admin.entity.JobInfo;
import com.wugui.hmt.admin.entity.JobLogReport;
import com.wugui.hmt.admin.entity.JobTemplate;
import com.wugui.hmt.admin.mapper.*;
import com.wugui.hmt.admin.service.DatasourceQueryService;
import com.wugui.hmt.admin.service.DataxJsonService;
import com.wugui.hmt.admin.service.JobService;
import com.wugui.hmt.admin.service.SubMetaDataService;
import com.wugui.hmt.admin.tool.meta.SubDb2DatabaseMeta;
import com.wugui.hmt.admin.tool.meta.SubDmDatabaseMeta;
import com.wugui.hmt.admin.tool.meta.SubOracleDatabaseMeta;
import com.wugui.hmt.admin.tool.meta.SubSqlServerDatabaseMeta;
import com.wugui.hmt.admin.util.DateFormatUtils;
import com.wugui.hmt.admin.util.JdbcConstants;
import com.wugui.hmt.admin.util.SubMetaUtil;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

import javax.annotation.Resource;
import java.io.IOException;
import java.text.MessageFormat;
import java.text.ParseException;
import java.util.*;


/*-------------------------------------------------------------------------
 *
 * JobServiceImpl.java
 *  core job action for xxl-job
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/service/impl/JobServiceImpl.java
 *
 *-----------------------------------------------
 */
@Service
public class JobServiceImpl implements JobService {
    private static Logger logger = LoggerFactory.getLogger(JobServiceImpl.class);

    @Resource
    private JobGroupMapper jobGroupMapper;
    @Resource
    private JobInfoMapper jobInfoMapper;
    @Resource
    private JobLogMapper jobLogMapper;
    @Resource
    private JobLogGlueMapper jobLogGlueMapper;
    @Resource
    private JobLogReportMapper jobLogReportMapper;
    @Resource
    private DatasourceQueryService datasourceQueryService;
    @Resource
    private JobTemplateMapper jobTemplateMapper;
    @Resource
    private DataxJsonService dataxJsonService;
    @Resource
    private SubMetaDataService subMetaDataService;

    @Override
    public Map<String, Object> pageList(int start, int length, int jobGroup, int triggerStatus, String jobDesc, String glueType, int userId, Integer[] projectIds, String handleCode, String metaType, String tableText, int isBigType) {

        // 表数组
        String[] tablesArray = StringUtils.isBlank(tableText) ? null : tableText.split("\n");
        // page list
        List<JobInfo> list = jobInfoMapper.pageList(start, length, jobGroup, triggerStatus, jobDesc, glueType, userId, projectIds, handleCode, metaType, tablesArray, isBigType);
        int list_count = jobInfoMapper.pageListCount(start, length, jobGroup, triggerStatus, jobDesc, glueType, userId, projectIds, handleCode, metaType, tablesArray, isBigType);

        // package result
        Map<String, Object> maps = new HashMap<>();
        maps.put("recordsTotal", list_count);        // 总记录数
        maps.put("recordsFiltered", list_count);    // 过滤后的总记录数
        maps.put("data", list);                    // 分页列表
        return maps;
    }

    @Override
    public List<JobInfo> list() {
        return jobInfoMapper.findAll();
    }

    @Override
    public ReturnT<String> add(JobInfo jobInfo) {
        // valid
        JobGroup group = jobGroupMapper.load(jobInfo.getJobGroup());
        if (group == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_choose") + I18nUtil.getString("jobinfo_field_jobgroup")));
        }
        /*if (!CronExpression.isValidExpression(jobInfo.getJobCron())) {
            return new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("jobinfo_field_cron_invalid"));
        }*/
        if (jobInfo.getGlueType().equals(GlueTypeEnum.DATAX.getDesc()) && jobInfo.getJobJson().trim().length() <= 2) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_jobjson")));
        }
        if (jobInfo.getJobDesc() == null || jobInfo.getJobDesc().trim().length() == 0) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_jobdesc")));
        }
        if (jobInfo.getUserId() == 0 ) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_author")));
        }
        if (ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null) == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorRouteStrategy") + I18nUtil.getString("system_invalid")));
        }
        if (ExecutorBlockStrategyEnum.match(jobInfo.getExecutorBlockStrategy(), null) == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorBlockStrategy") + I18nUtil.getString("system_invalid")));
        }
        if (GlueTypeEnum.match(jobInfo.getGlueType()) == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_gluetype") + I18nUtil.getString("system_invalid")));
        }
        if ((GlueTypeEnum.DATAX == GlueTypeEnum.match(jobInfo.getGlueType()) || GlueTypeEnum.JAVA_BEAN == GlueTypeEnum.match(jobInfo.getGlueType()))
            && (jobInfo.getExecutorHandler() == null || jobInfo.getExecutorHandler().trim().length() == 0)) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + "JobHandler"));
        }


        if (StringUtils.isBlank(jobInfo.getReplaceParamType()) || !DateFormatUtils.formatList().contains(jobInfo.getReplaceParamType())) {
            jobInfo.setReplaceParamType(DateFormatUtils.TIMESTAMP);
        }

        // fix "\r" in shell
        if (GlueTypeEnum.GLUE_SHELL == GlueTypeEnum.match(jobInfo.getGlueType()) && jobInfo.getGlueSource() != null) {
            jobInfo.setGlueSource(jobInfo.getGlueSource().replaceAll("\r", ""));
        }

        // ChildJobId valid
        if (jobInfo.getChildJobId() != null && jobInfo.getChildJobId().trim().length() > 0) {
            String[] childJobIds = jobInfo.getChildJobId().split(",");
            for (String childJobIdItem : childJobIds) {
                if (StringUtils.isNotBlank(childJobIdItem) && isNumeric(childJobIdItem) && Integer.parseInt(childJobIdItem) > 0) {
                    JobInfo childJobInfo = jobInfoMapper.loadById(Integer.parseInt(childJobIdItem));
                    if (childJobInfo == null) {
                        return new ReturnT<String>(ReturnT.FAIL_CODE,
                            MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId") + "({0})" + I18nUtil.getString("system_not_found")), childJobIdItem));
                    }
                } else {
                    return new ReturnT<String>(ReturnT.FAIL_CODE,
                        MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId") + "({0})" + I18nUtil.getString("system_invalid")), childJobIdItem));
                }
            }

            // join , avoid "xxx,,"
            String temp = "";
            for (String item : childJobIds) {
                temp += item + ",";
            }
            temp = temp.substring(0, temp.length() - 1);

            jobInfo.setChildJobId(temp);
        }

        //查询当前表中是否包含大字段
        if(jobInfo.getReaderDataSource().equals("oracle") || jobInfo.getReaderDataSource().equals("sqlserver") || jobInfo.getReaderDataSource().equals("dm") || jobInfo.getReaderDataSource().equals("db2")) {
            String tableName = jobInfo.getJobDesc().replaceAll(jobInfo.getReaderSchema() + "\\.", "");
            List<Map<String, String>> bigTypeTableList;
            if(jobInfo.getReaderDataSource().equals("oracle")){
                bigTypeTableList = subMetaDataService.getExistBigTypeTables(SubOracleDatabaseMeta.getExistBigTypeTables(jobInfo.getReaderSchema(), tableName), jobInfo.getReaderDatasourceId());
            } else if(jobInfo.getReaderDataSource().equals("sqlserver")){
                bigTypeTableList = subMetaDataService.getExistBigTypeTables(SubSqlServerDatabaseMeta.getExistBigTypeTables(jobInfo.getReaderSchema(), tableName), jobInfo.getReaderDatasourceId());
            } else if(jobInfo.getReaderDataSource().equals("dm")){
                bigTypeTableList = subMetaDataService.getExistBigTypeTables(SubDmDatabaseMeta.getExistBigTypeTables(jobInfo.getReaderSchema(), tableName), jobInfo.getReaderDatasourceId());
            } else{
                bigTypeTableList = subMetaDataService.getExistBigTypeTables(SubDb2DatabaseMeta.getExistBigTypeTables(jobInfo.getReaderSchema(), tableName), jobInfo.getReaderDatasourceId());
            }
            if (bigTypeTableList.size() > 0) {
                jobInfo.setIsBigType(1);
            }
        }

        // add in db
        jobInfo.setAddTime(new Date());
        jobInfo.setJobJson(jobInfo.getJobJson());
        jobInfo.setUpdateTime(new Date());
        jobInfo.setGlueUpdatetime(new Date());
        jobInfoMapper.save(jobInfo);
        if (jobInfo.getId() < 1) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_add") + I18nUtil.getString("system_fail")));
        }

        return new ReturnT<>(String.valueOf(jobInfo.getId()));
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
    public ReturnT<String> update(JobInfo jobInfo) {

        // valid
        /*if (!CronExpression.isValidExpression(jobInfo.getJobCron())) {
            return new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("jobinfo_field_cron_invalid"));
        }*/
        if (jobInfo.getGlueType().equals(GlueTypeEnum.DATAX.getDesc()) && jobInfo.getJobJson().trim().length() <= 2) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_jobjson")));
        }
        if (jobInfo.getJobDesc() == null || jobInfo.getJobDesc().trim().length() == 0) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_jobdesc")));
        }

        if (jobInfo.getProjectId() == 0) {
            return new ReturnT<String>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_jobproject")));
        }
        if (jobInfo.getUserId() == 0) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobinfo_field_author")));
        }
        if (ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null) == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorRouteStrategy") + I18nUtil.getString("system_invalid")));
        }
        if (ExecutorBlockStrategyEnum.match(jobInfo.getExecutorBlockStrategy(), null) == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_executorBlockStrategy") + I18nUtil.getString("system_invalid")));
        }

        // ChildJobId valid
        if (jobInfo.getChildJobId() != null && jobInfo.getChildJobId().trim().length() > 0) {
            String[] childJobIds = jobInfo.getChildJobId().split(",");
            for (String childJobIdItem : childJobIds) {
                if (childJobIdItem != null && childJobIdItem.trim().length() > 0 && isNumeric(childJobIdItem)) {
                    JobInfo childJobInfo = jobInfoMapper.loadById(Integer.parseInt(childJobIdItem));
                    if (childJobInfo == null) {
                        return new ReturnT<String>(ReturnT.FAIL_CODE,
                            MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId") + "({0})" + I18nUtil.getString("system_not_found")), childJobIdItem));
                    }
                } else {
                    return new ReturnT<String>(ReturnT.FAIL_CODE,
                        MessageFormat.format((I18nUtil.getString("jobinfo_field_childJobId") + "({0})" + I18nUtil.getString("system_invalid")), childJobIdItem));
                }
            }

            // join , avoid "xxx,,"
            String temp = "";
            for (String item : childJobIds) {
                temp += item + ",";
            }
            temp = temp.substring(0, temp.length() - 1);

            jobInfo.setChildJobId(temp);
        }

        // group valid
        JobGroup jobGroup = jobGroupMapper.load(jobInfo.getJobGroup());
        if (jobGroup == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_jobgroup") + I18nUtil.getString("system_invalid")));
        }

        // stage job info
        JobInfo exists_jobInfo = jobInfoMapper.loadById(jobInfo.getId());
        if (exists_jobInfo == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("jobinfo_field_id") + I18nUtil.getString("system_not_found")));
        }

        // next trigger time (5s后生效，避开预读周期)
        long nextTriggerTime = exists_jobInfo.getTriggerNextTime();
        if (exists_jobInfo.getTriggerStatus() == 1 && !jobInfo.getJobCron().equals(exists_jobInfo.getJobCron())) {
            try {
                Date nextValidTime = new CronExpression(jobInfo.getJobCron()).getNextValidTimeAfter(new Date(System.currentTimeMillis() + JobScheduleHelper.PRE_READ_MS));
                if (nextValidTime == null) {
                    return new ReturnT<String>(ReturnT.FAIL_CODE, I18nUtil.getString("jobinfo_field_cron_never_fire"));
                }
                nextTriggerTime = nextValidTime.getTime();
            } catch (ParseException e) {
                logger.error(e.getMessage(), e);
                return new ReturnT<String>(ReturnT.FAIL_CODE, I18nUtil.getString("jobinfo_field_cron_invalid") + " | " + e.getMessage());
            }
        }

        BeanUtils.copyProperties(jobInfo, exists_jobInfo);
        if (StringUtils.isBlank(jobInfo.getReplaceParamType())) {
            jobInfo.setReplaceParamType(DateFormatUtils.TIMESTAMP);
        }
        exists_jobInfo.setTriggerNextTime(nextTriggerTime);
        exists_jobInfo.setUpdateTime(new Date());

        if (GlueTypeEnum.DATAX.getDesc().equals(jobInfo.getGlueType()) || GlueTypeEnum.JAVA_BEAN.getDesc().equals(jobInfo.getGlueType())) {
            exists_jobInfo.setJobJson(jobInfo.getJobJson());
            exists_jobInfo.setGlueSource(null);
        } else {
            exists_jobInfo.setGlueSource(jobInfo.getGlueSource());
            exists_jobInfo.setJobJson(null);
        }
        exists_jobInfo.setGlueUpdatetime(new Date());
        jobInfoMapper.update(exists_jobInfo);


        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> remove(int id) {
        JobInfo xxlJobInfo = jobInfoMapper.loadById(id);
        if (xxlJobInfo == null) {
            return ReturnT.SUCCESS;
        }

        jobInfoMapper.delete(id);
        jobLogMapper.delete(id);
        jobLogGlueMapper.deleteByJobId(id);
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> start(int id) {
        JobInfo xxlJobInfo = jobInfoMapper.loadById(id);

        // next trigger time (5s后生效，避开预读周期)
        long nextTriggerTime = 0;
        try {
            Date nextValidTime = new CronExpression(xxlJobInfo.getJobCron()).getNextValidTimeAfter(new Date(System.currentTimeMillis() + JobScheduleHelper.PRE_READ_MS));
            if (nextValidTime == null) {
                return new ReturnT<String>(ReturnT.FAIL_CODE, I18nUtil.getString("jobinfo_field_cron_never_fire"));
            }
            nextTriggerTime = nextValidTime.getTime();
        } catch (ParseException e) {
            logger.error(e.getMessage(), e);
            return new ReturnT<String>(ReturnT.FAIL_CODE, I18nUtil.getString("jobinfo_field_cron_invalid") + " | " + e.getMessage());
        }

        xxlJobInfo.setTriggerStatus(1);
        xxlJobInfo.setTriggerLastTime(0);
        xxlJobInfo.setTriggerNextTime(nextTriggerTime);

        xxlJobInfo.setUpdateTime(new Date());
        jobInfoMapper.update(xxlJobInfo);
        return ReturnT.SUCCESS;
    }


    @Override
    public ReturnT<String> singleStart(int id) {

        JobInfo jobInfo = jobInfoMapper.loadById(id);

        jobInfo.setLastHandleCode(1);
        jobInfo.setTriggerStatus(1);
        jobInfo.setUpdateTime(new Date());
        jobInfoMapper.update(jobInfo);
        // 1、trigger
        JobTriggerPoolHelper.trigger(jobInfo.getId(), TriggerTypeEnum.CRON, -1, null, null);
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> batchStart(String jobDesc, Integer[] projectIds, String glueType, String metaType, String tableText, int isBigType) {
        logger.info(">>>>>>>>>>> batchStart : projectIds = " + StringUtils.join(projectIds, ","));
        // 表数组
        String[] tablesArray = StringUtils.isBlank(tableText) ? null : tableText.split("\n");
        jobInfoMapper.batchStart(jobDesc, projectIds, glueType, metaType, tablesArray, isBigType);
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> stop(int id) {
        JobInfo jobInfo = jobInfoMapper.loadById(id);

        jobInfo.setTriggerStatus(0);
        jobInfo.setTriggerLastTime(0);
        jobInfo.setTriggerNextTime(0);

        jobInfo.setUpdateTime(new Date());
        jobInfoMapper.update(jobInfo);
        return ReturnT.SUCCESS;
    }

    @Override
    public Map<String, Object> dashboardInfo() {

        int jobInfoCount = jobInfoMapper.findAllCount();
        int jobLogCount = 0;
        int jobLogSuccessCount = 0;
        JobLogReport jobLogReport = jobLogReportMapper.queryLogReportTotal();
        if (jobLogReport != null) {
            jobLogCount = jobLogReport.getRunningCount() + jobLogReport.getSucCount() + jobLogReport.getFailCount();
            jobLogSuccessCount = jobLogReport.getSucCount();
        }

        // executor count
        Set<String> executorAddressSet = new HashSet<>();
        List<JobGroup> groupList = jobGroupMapper.findAll();

        if (groupList != null && !groupList.isEmpty()) {
            for (JobGroup group : groupList) {
                if (group.getRegistryList() != null && !group.getRegistryList().isEmpty()) {
                    executorAddressSet.addAll(group.getRegistryList());
                }
            }
        }

        int executorCount = executorAddressSet.size();

        Map<String, Object> dashboardMap = new HashMap<>();
        dashboardMap.put("jobInfoCount", jobInfoCount);
        dashboardMap.put("jobLogCount", jobLogCount);
        dashboardMap.put("jobLogSuccessCount", jobLogSuccessCount);
        dashboardMap.put("executorCount", executorCount);
        return dashboardMap;
    }

    @Override
    public ReturnT<Map<String, Object>> chartInfo() {
        // process
        List<String> triggerDayList = new ArrayList<String>();
        List<Integer> triggerDayCountRunningList = new ArrayList<Integer>();
        List<Integer> triggerDayCountSucList = new ArrayList<Integer>();
        List<Integer> triggerDayCountFailList = new ArrayList<Integer>();
        int triggerCountRunningTotal = 0;
        int triggerCountSucTotal = 0;
        int triggerCountFailTotal = 0;
        //待执行总数
        int triggerCountAwaitTotal = 0;

        List<JobLogReport> logReportList = jobLogReportMapper.queryLogReport(DateUtil.addDays(new Date(), -7), new Date());

        if (logReportList != null && logReportList.size() > 0) {
            for (JobLogReport item : logReportList) {
                String day = DateUtil.formatDate(item.getTriggerDay());
                int triggerDayCountRunning = item.getRunningCount();
                int triggerDayCountSuc = item.getSucCount();
                int triggerDayCountFail = item.getFailCount();

                triggerDayList.add(day);
                triggerDayCountRunningList.add(triggerDayCountRunning);
                triggerDayCountSucList.add(triggerDayCountSuc);
                triggerDayCountFailList.add(triggerDayCountFail);

                triggerCountRunningTotal += triggerDayCountRunning;
                triggerCountSucTotal += triggerDayCountSuc;
                triggerCountFailTotal += triggerDayCountFail;
            }
        } else {
            for (int i = -6; i <= 0; i++) {
                triggerDayList.add(DateUtil.formatDate(DateUtil.addDays(new Date(), i)));
                triggerDayCountRunningList.add(0);
                triggerDayCountSucList.add(0);
                triggerDayCountFailList.add(0);
            }
        }

        Map<String, Object> result = new HashMap<>();
        result.put("triggerDayList", triggerDayList);
        result.put("triggerDayCountRunningList", triggerDayCountRunningList);
        result.put("triggerDayCountSucList", triggerDayCountSucList);
        result.put("triggerDayCountFailList", triggerDayCountFailList);

        result.put("triggerCountRunningTotal", triggerCountRunningTotal);
        result.put("triggerCountSucTotal", triggerCountSucTotal);
        result.put("triggerCountFailTotal", triggerCountFailTotal);
        //获取待执行的总数
        triggerCountAwaitTotal = jobInfoMapper.getJobInfoCountAwaitTotal(DateUtil.addDays(new Date(), -7), new Date());
        result.put("triggerCountAwaitTotal", triggerCountAwaitTotal);

        return new ReturnT<>(result);
    }


    @Override
    @Transactional(rollbackFor = Exception.class)
    public ReturnT<String> batchAdd(DataXBatchJsonBuildDto dto) throws IOException {
        String key = "system_please_choose";
        List<String> rdTables = dto.getReaderTables();
        List<String> wrTables = dto.getWriterTables();

        if (dto.getReaderDatasourceId() == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString(key) + I18nUtil.getString("jobinfo_field_readerDataSource"));
        }
        if (dto.getWriterDatasourceId() == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString(key) + I18nUtil.getString("jobinfo_field_writerDataSource"));
        }
        if (rdTables.size() != wrTables.size()) {
            return new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("json_build_inconsistent_number_r_w_tables"));
        }

        Map<String, String> tbMap = new HashMap<>();

        for (int i = 0; i < wrTables.size(); i++) {
            String tbName = "";
            if(wrTables.get(i).contains(".")){
                String[] arr = wrTables.get(i).split("\\.");
                tbName = arr[1].toLowerCase();
            }else{
                tbName = wrTables.get(i).toLowerCase();
            }

            tbMap.put(tbName, wrTables.get(i));
        }

        DataXJsonBuildDto jsonBuild = new DataXJsonBuildDto();

        List<String> rColumns;
        List<String> wColumns;

        //查询当前schema下包含大字段的表集合
        List<Map<String, String>> bigTypeTableList = new ArrayList<>();
        if(dto.getReaderDataSource().equals("oracle") || dto.getReaderDataSource().equals("sqlserver") || dto.getReaderDataSource().equals("dm") || dto.getReaderDataSource().equals("db2")) {
            if(dto.getReaderDataSource().equals("oracle")){
                bigTypeTableList = subMetaDataService.getExistBigTypeTables(SubOracleDatabaseMeta.getExistBigTypeTables(dto.getReaderSchema(), ""), dto.getReaderDatasourceId());
            } else if(dto.getReaderDataSource().equals("sqlserver")){
                bigTypeTableList = subMetaDataService.getExistBigTypeTables(SubSqlServerDatabaseMeta.getExistBigTypeTables(dto.getReaderSchema(), ""), dto.getReaderDatasourceId());
            } else if(dto.getReaderDataSource().equals("dm")){
                bigTypeTableList = subMetaDataService.getExistBigTypeTables(SubDmDatabaseMeta.getExistBigTypeTables(dto.getReaderSchema(), ""), dto.getReaderDatasourceId());
            } else{
                bigTypeTableList = subMetaDataService.getExistBigTypeTables(SubDb2DatabaseMeta.getExistBigTypeTables(dto.getReaderSchema(), ""), dto.getReaderDatasourceId());
            }
        }

        try {
            for (int i = 0; i < rdTables.size(); i++) {
                String tbName = "";
                if(rdTables.get(i).contains(".")){
                    String[] arr = rdTables.get(i).split("\\.");
                    tbName = arr[1].toLowerCase();
                }else{
                    tbName = rdTables.get(i).toLowerCase();
                }

                if(tbMap.containsKey(tbName)){
                    //源库对应的表名，在目标库中存在
                    String wrTable = tbMap.get(tbName);

                    rColumns = datasourceQueryService.getColumns(dto.getReaderDatasourceId(), dto.getReaderSchema(), rdTables.get(i));
                    wColumns = datasourceQueryService.getColumns(dto.getWriterDatasourceId(), dto.getWriterSchema(), wrTable);

                    jsonBuild.setReaderDatasourceId(dto.getReaderDatasourceId());
                    jsonBuild.setWriterDatasourceId(dto.getWriterDatasourceId());

                    jsonBuild.setReaderSchema(dto.getReaderSchema());
                    jsonBuild.setWriterSchema(dto.getWriterSchema());

                    jsonBuild.setReaderColumns(rColumns);
                    jsonBuild.setWriterColumns(wColumns);

                    jsonBuild.setRdbmsReader(dto.getRdbmsReader());
                    jsonBuild.setRdbmsWriter(dto.getRdbmsWriter());

                    List<String> rdTable = new ArrayList<>();
                    rdTable.add(rdTables.get(i));
                    jsonBuild.setReaderTables(rdTable);

                    List<String> wdTable = new ArrayList<>();
                    wdTable.add(wrTable);
                    jsonBuild.setWriterTables(wdTable);

                    String json = dataxJsonService.buildJobJson(jsonBuild);

                    JobTemplate jobTemplate = jobTemplateMapper.loadById(dto.getTemplateId());
                    JobInfo jobInfo = new JobInfo();
                    BeanUtils.copyProperties(jobTemplate, jobInfo);
                    jobInfo.setJobJson(json);
                    jobInfo.setJobDesc(rdTables.get(i));
                    jobInfo.setAddTime(new Date());
                    jobInfo.setUpdateTime(new Date());
                    jobInfo.setGlueUpdatetime(new Date());

                    //查询表中是否包含大字段的数据类型
                    if(dto.getReaderDataSource().equals("oracle") || dto.getReaderDataSource().equals("sqlserver") || dto.getReaderDataSource().equals("dm") || dto.getReaderDataSource().equals("db2")) {
                        String finalTbName = rdTables.get(i).replaceAll(dto.getReaderSchema() + "\\.", "");
                        boolean ifExist = bigTypeTableList.stream().anyMatch(map -> map.get("TABLE_NAME").equals(finalTbName));
                        if (ifExist) {
                            jobInfo.setIsBigType(1);
                        }
                    }

                    jobInfoMapper.save(jobInfo);
                }else{
                    throw new Exception(tbName);
                }
            }
        }catch(Exception e){
            logger.error("批量添加任务失败：",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("json_build_reader_table_notfound_in_write") + "," + e);
        }

        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> batchStartFailTask(String jobDesc, Integer[] projectIds, String glueType, String metaType, String tableText, int isBigType) {
        logger.info(">>>>>>>>>>> batchStartFailTask : projectIds = " + StringUtils.join(projectIds, ","));
        // 表数组
        String[] tablesArray = StringUtils.isBlank(tableText) ? null : tableText.split("\n");
        jobInfoMapper.batchStartFailTask(jobDesc, projectIds, glueType, metaType, tablesArray, isBigType);
        return ReturnT.SUCCESS;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public ReturnT<String> batchDeleteTask(String jobDesc, Integer[] projectIds, String glueType, String handleCode, String metaType, String tableText, int isBigType) {
        // 表数组
        String[] tablesArray = StringUtils.isBlank(tableText) ? null : tableText.split("\n");
        // 根据任务id删除对应的日志
        jobLogMapper.batchDeleteLogByTaskInfo(jobDesc, projectIds, glueType, handleCode, metaType, tablesArray, isBigType);
        // 删除任务
        jobInfoMapper.batchDeleteTask(jobDesc, projectIds, glueType, handleCode, metaType, tablesArray, isBigType);
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> batchStopTask(String jobDesc, Integer[] projectIds, String glueType, String metaType, String tableText, int isBigType) {
        logger.info(">>>>>>>>>>> batchStopTask : projectIds = " + StringUtils.join(projectIds, ","));
        // 表数组
        String[] tablesArray = StringUtils.isBlank(tableText) ? null : tableText.split("\n");
        jobInfoMapper.batchStopTask(jobDesc, projectIds, glueType, metaType, tablesArray, isBigType);
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> batchSetDataWay(int jobGroup, int triggerStatus, String jobDesc, String glueType, Integer[] projectIds, String handleCode, String metaType, String tableText, int isBigType, String writerType) {
        // 表数组
        String[] tablesArray = StringUtils.isBlank(tableText) ? null : tableText.split("\n");
        // 查询需要修改的数据
        List<JobInfo> jobInfoList = jobInfoMapper.getPrepareSetList(jobGroup, triggerStatus, jobDesc, glueType, projectIds, handleCode, metaType, tablesArray, isBigType);
        for (JobInfo jobInfo:jobInfoList) {
            //先将字符串转化为LinkedHashMap,然后定义有序的json对象,保证原来的排序
            LinkedHashMap<String, Object> json = JSON.parseObject(jobInfo.getJobJson(),LinkedHashMap.class, Feature.OrderedField);
            JSONObject jsonObject=new JSONObject(true);
            jsonObject.putAll(json);
            jsonObject.getJSONObject("job").getJSONArray("content").getJSONObject(0).getJSONObject("writer").put("name", writerType);
            jobInfo.setJobJson(jsonObject.toJSONString());
        }
        jobInfoMapper.batchSetDataWay(jobInfoList);
        return ReturnT.SUCCESS;
    }

    @Override
    public int getJobCount(int projectId, String metaTypeId, int triggerStatus, Integer[] handleCode) {
        return jobInfoMapper.getJobCount(projectId, metaTypeId, triggerStatus, handleCode);
    }

    @Override
    public ReturnT<String> automationBatchStart(int projectId, String metaTypeId) {
        jobInfoMapper.automationBatchStart(projectId, metaTypeId);
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> automationBatchSetDataWay(int projectId, int triggerStatus, String handleCode, String writerType) {
        // 查询需要修改的数据
        List<JobInfo> jobInfoList = jobInfoMapper.getAutomationPrepareSetList(projectId, triggerStatus, handleCode, writerType);
        for (JobInfo jobInfo:jobInfoList) {
            //先将字符串转化为LinkedHashMap,然后定义有序的json对象,保证原来的排序
            LinkedHashMap<String, Object> json = JSON.parseObject(jobInfo.getJobJson(),LinkedHashMap.class, Feature.OrderedField);
            JSONObject jsonObject=new JSONObject(true);
            jsonObject.putAll(json);
            jsonObject.getJSONObject("job").getJSONArray("content").getJSONObject(0).getJSONObject("writer").put("name", writerType);
            jobInfo.setJobJson(jsonObject.toJSONString());
        }
        // 批量设置后自动执行任务
        jobInfoMapper.automationBatchSetDataWay(jobInfoList);
        return ReturnT.SUCCESS;
    }

}
