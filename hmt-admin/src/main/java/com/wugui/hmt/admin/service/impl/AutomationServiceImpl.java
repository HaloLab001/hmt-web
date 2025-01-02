package com.wugui.hmt.admin.service.impl;

import java.io.IOException;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.admin.dto.DataXBatchJsonBuildDto;
import com.wugui.hmt.admin.entity.JobProject;
import com.wugui.hmt.admin.entity.JobTemplate;
import com.wugui.hmt.admin.service.AutomationService;
import com.wugui.hmt.admin.service.JobProjectService;
import com.wugui.hmt.admin.service.JobService;
import com.wugui.hmt.admin.service.JobTemplateService;
import com.wugui.hmt.admin.service.SubMetaDataService;
import com.wugui.hmt.admin.tool.pojo.AutomationCreatePojo;
import com.wugui.hmt.admin.tool.pojo.CreateMetaPojo;
import com.wugui.hmt.admin.tool.pojo.MetaTypePojo;
import com.wugui.hmt.admin.util.SubMetaUtil;

/*-------------------------------------------------------------------------
 *
 * AutomationServiceImpl.java
 *  AutomationServiceImpl类
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/service/impl/AutomationServiceImpl.java
 *
 *-----------------------------------------------
 */
@Service
public class AutomationServiceImpl implements AutomationService{

    @Autowired
    JobProjectService jobProjectService;

    @Autowired
    JobTemplateService jobTemplateService;

    @Autowired
    SubMetaDataService subMetaDataService;

    @Autowired
    JobService jobService;

    @Override
    public ReturnT<String> add(AutomationCreatePojo automationCreatePojo) {
        //需要迁移的schema
        List<String> schemaList = automationCreatePojo.getSchemaList();
        for(int i = 0; i < schemaList.size(); i++){
            //源端schema名称
            String readerSchema = schemaList.get(i);
            //添加项目
            JobProject jobProject = new JobProject();
            jobProject.setName(readerSchema + "-自动化迁移任务");
            jobProject.setDescription(automationCreatePojo.getDescription());
            jobProject.setUserId(automationCreatePojo.getUserId());
            jobProject.setFlag(1);
            jobProject.setReaderDatasourceId(automationCreatePojo.getDatasourceId_1());
            jobProject.setWriterDatasourceId(automationCreatePojo.getDatasourceId_2());
            jobProject.setReaderSchema(readerSchema);
            jobProjectService.save(jobProject);
            //添加任务模板
            JobTemplate jobTemplate = new JobTemplate();
            jobTemplate.setProjectId(jobProject.getId());
            jobTemplate.setJobGroup(automationCreatePojo.getJobGroup());
            jobTemplate.setJvmParam(automationCreatePojo.getJvmParam());
            jobTemplate.setJobDesc(readerSchema + "-自动化迁移任务");
            jobTemplate.setUserId(automationCreatePojo.getUserId());
            jobTemplate.setExecutorRouteStrategy("RANDOM");
            jobTemplate.setExecutorHandler("executorJobHandler");
            jobTemplate.setExecutorBlockStrategy("DISCARD_LATER");
            jobTemplate.setGlueType("DATAX");
            //取回任务模板id
            ReturnT<String> jobTemplateReturnT = jobTemplateService.add(jobTemplate);
            //生成元数据任务的配置参数
            CreateMetaPojo createMetaPojo = new CreateMetaPojo();
            createMetaPojo.setUserId(automationCreatePojo.getUserId());
            createMetaPojo.setTemplateId(Long.parseLong(jobTemplateReturnT.getContent()));
            createMetaPojo.setDatasourceId_1(automationCreatePojo.getDatasourceId_1());
            createMetaPojo.setDatasourceId_2(automationCreatePojo.getDatasourceId_2());
            createMetaPojo.setTablePartNameSwitch(automationCreatePojo.getTablePartNameSwitch());
            createMetaPojo.setTableAndColSwitch(automationCreatePojo.getTableAndColSwitch());
            createMetaPojo.setMysqlCommandSwitch(automationCreatePojo.getMysqlCommandSwitch());
            createMetaPojo.setMysqlIp(automationCreatePojo.getMysqlIp());
            createMetaPojo.setMysqlPort(automationCreatePojo.getMysqlPort());
            createMetaPojo.setMysqlUser(automationCreatePojo.getMysqlUser());
            createMetaPojo.setMysqlPwd(automationCreatePojo.getMysqlPwd());
            createMetaPojo.setLogFileName(automationCreatePojo.getLogFileName());
            createMetaPojo.setSchema(readerSchema);
            List<MetaTypePojo> metaTypePojos = SubMetaUtil.getMetaTypePojoList();
            //区分Oracle和MySQL的元数据类型
            createMetaPojo.setMetaIds(metaTypePojos.stream().mapToInt(MetaTypePojo::getMetaTypeId).toArray());
            //生成元数据任务
            subMetaDataService.add(createMetaPojo);
        }
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> triggerStart(int id) {
        //目前只允许同时调度1个任务
        List<JobProject> jobProjectList = jobProjectService.lambdaQuery().eq(JobProject::getTriggerStatus, 1).list();
        if(!jobProjectList.isEmpty()){
            return new ReturnT<String>(101, "启动调度失败，目前只允许同时调度1个任务");
        }
        jobProjectService.lambdaUpdate().set(JobProject::getTriggerStatus, 1).set(JobProject::getRunningStatus, 0).eq(JobProject::getId, id).update();
        return ReturnT.SUCCESS;
    }

    @Override
    public ReturnT<String> triggerStop(int id) {
        jobProjectService.lambdaUpdate().set(JobProject::getTriggerStatus, 0).eq(JobProject::getId, id).update();
        return ReturnT.SUCCESS;
    }

}
