package com.wugui.hmt.admin.service.impl;

import cn.hutool.core.util.ObjectUtil;
import com.google.common.collect.Lists;
import com.ibm.db2.jcc.am.bo;
import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.admin.core.conf.JobAdminConfig;
import com.wugui.hmt.admin.core.util.I18nUtil;
import com.wugui.hmt.admin.entity.JobDatasource;
import com.wugui.hmt.admin.entity.JobInfo;
import com.wugui.hmt.admin.mapper.JobInfoMapper;
import com.wugui.hmt.admin.mapper.SubMetaDataMapper;
import com.wugui.hmt.admin.service.JobDatasourceService;
import com.wugui.hmt.admin.service.SubMetaDataMySQLService;
import com.wugui.hmt.admin.service.SubMetaDataOracleService;
import com.wugui.hmt.admin.service.SubMetaDataService;
import com.wugui.hmt.admin.tool.pojo.*;
import com.wugui.hmt.admin.tool.query.BaseQueryTool;
import com.wugui.hmt.admin.tool.query.QueryToolFactory;
import com.wugui.hmt.admin.util.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import javax.annotation.Resource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


/*-------------------------------------------------------------------------
 *
 * SubMetaDataServiceImpl.java
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/service/impl/SubMetaDataServiceImpl.java
 *
 *-----------------------------------------------
 */
@Transactional(rollbackFor = Exception.class)
@Service
public class SubMetaDataServiceImpl implements SubMetaDataService {

    protected static final Logger logger = LoggerFactory.getLogger(SubMetaDataServiceImpl.class);

    @Resource
    private SubMetaDataMapper subMetaDataMapper;

    @Resource
    private JobInfoMapper jobInfoMapper;

    @Resource
    private JobDatasourceService jobDatasourceService;

    @Resource
    private SubMetaDataOracleService subMetaDataOracleService;

    @Resource
    private SubMetaDataMySQLService subMetaDataMySQLService;

    @Override
    public List<MetaTypePojo> dashboardList(Integer[] projectIds) {
        // 获取元数据类型集合
        List<MetaTypePojo> metaTypePojoList = SubMetaUtil.getMetaTypePojoList();
        // 获取统计集合
        List<MetaTypePojo> metaTypePojoList2 = subMetaDataMapper.getMetaDashboardList(projectIds);
        for (MetaTypePojo mInfo : metaTypePojoList) {
            MetaTypePojo metaTypePojo = metaTypePojoList2.stream()
                    .filter(m -> m.getMetaTypeId() == mInfo.getMetaTypeId()).findFirst().orElse(null);
            if (metaTypePojo != null) {
                mInfo.setAllTotal(metaTypePojo.getAllTotal());
                mInfo.setAwaitTotal(metaTypePojo.getAwaitTotal());
                mInfo.setRunningTotal(metaTypePojo.getRunningTotal());
                mInfo.setSucTotal(metaTypePojo.getSucTotal());
                mInfo.setFailTotal(metaTypePojo.getFailTotal());
            }
        }
        return metaTypePojoList;
    }

    @Override
    public DataDashboardPojo dataDashboardList() {
        return subMetaDataMapper.getDataDashboardInfo();
    }

    @Override
    public String logDetail(String logFileName) {
        StringBuilder contentBuilder = new StringBuilder();
        InputStream inputStream = this.getClass().getResourceAsStream("/meta/log/" +
                logFileName + ".log");
        if (inputStream == null) {
            return "no log file";
        }
        contentBuilder.append(
                "---------------------------------------------------------------------任务正在执行中...--------------------------------------------------------------------"
                        + "\n");
        try (BufferedReader reader = new BufferedReader(new
                InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                contentBuilder.append(line).append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return contentBuilder.toString();
    }

    @Override
    public Map<String, Object> checkTableDifference(SubCheckTablePojo subCheckTablePojo) {
        Map<String, Object> map = new HashMap<>();
        List<String> readerTables = subCheckTablePojo.getReaderTables();
        List<String> writerTables = subCheckTablePojo.getWriterTables();
        // readerTables去掉schema
        UnaryOperator<String> unaryOpt = pn -> SubMetaUtil.modifyName(pn, subCheckTablePojo.getReaderSchema() + "\\.");
        readerTables.replaceAll(unaryOpt);
        // writerTables去掉schema
        unaryOpt = pn -> SubMetaUtil.modifyName(pn, subCheckTablePojo.getWriterSchema() + "\\.");
        writerTables.replaceAll(unaryOpt);
        // readerTables转换大写
        readerTables = readerTables.stream().map(String::toUpperCase).collect(Collectors.toList());
        // writerTables转换大写
        writerTables = writerTables.stream().map(String::toUpperCase).collect(Collectors.toList());
        // 源端特有表
        List<String> rTables = SubMetaUtil.listRemoveSame(readerTables, writerTables);
        // 目标端特有表
        List<String> wTables = SubMetaUtil.listRemoveSame(writerTables, readerTables);
        Collections.sort(rTables);
        Collections.sort(wTables);
        map.put("readerTables", rTables);
        map.put("writerTables", wTables);
        return map;
    }

    @Override
    public Map<String, Object> getVersionInfo() {
        Map<String, Object> map = new HashMap<>();
        map.put("version", JobAdminConfig.getAdminConfig().getVersion());
        return map;
    }

    @Override
    public List<Map<String, String>> getExistBigTypeTables(String executeSql, Long datasourceId) {
        return getMetaDataPublic(datasourceId, executeSql, SubMetaEnum.TABLE_NAME_1.getTypeId());
    }

    @Override
    public List<Map<String, String>> getNoPrimaryKeyTables(String executeSql, Long datasourceId) {
        return getMetaDataPublic(datasourceId, executeSql, SubMetaEnum.TABLE_NAME_1.getTypeId());
    }

    /**
     * 添加任务
     * 
     * @param jobInfo
     * @param startPointMap
     * @param jobDesc
     * @param
     */
    @Override
    public void jsonBuild(JobInfo jobInfo, Map<String, String> startPointMap, String jobDesc, String sqlInfo,
            int metaId, int buildType) {
        // 最终脚本
        String shellText = "";
        if (buildType == SubMetaEnum.BUILD_TYPE_1.getTypeId()) {
            // 拼接
            shellText = startPointMap.get("beginTest") + sqlInfo + startPointMap.get("endTest");
        } else if (buildType == SubMetaEnum.BUILD_TYPE_2.getTypeId()) {
            shellText = sqlInfo;
        }
        // add in db
        jobInfo.setJobDesc(jobDesc);
        jobInfo.setAddTime(new Date());
        jobInfo.setGlueSource(shellText);
        jobInfo.setUpdateTime(new Date());
        jobInfo.setGlueUpdatetime(new Date());
        // 此处暂用空闲字段存储元数据类型 job_cron
        jobInfo.setJobCron(String.valueOf(metaId));
        jobInfoMapper.save(jobInfo);
    }

    @Override
    public ReturnT<String> add(CreateMetaPojo createMetaInfo) {
        // 获取数据源对象
        JobDatasource datasource = jobDatasourceService.getById(createMetaInfo.getDatasourceId_1());
        ReturnT<String> returnT;
        if (datasource.getDatasource().equals("oracle")) {
            returnT = subMetaDataOracleService.add(createMetaInfo);
        } else if (datasource.getDatasource().equals("mysql")) {
            returnT = subMetaDataMySQLService.add(createMetaInfo);
        } else {
            return new ReturnT<>(101, "该数据库暂不支持元数据迁移");
        }
        return returnT;
    }

    public List<Map<String, String>> getMetaDataPublic(Long id, String executeSql, int type) {
        // 获取数据源对象
        JobDatasource datasource = jobDatasourceService.getById(id);
        // queryTool组装
        if (ObjectUtil.isNull(datasource)) {
            return Lists.newArrayList();
        }
        BaseQueryTool qTool = QueryToolFactory.getByDbType(datasource);
        List<Map<String, String>> metaDataPojoList = qTool.getMetaPublicSQLs(executeSql, type);
        return metaDataPojoList;
    }

    @Override
    public Map<String, Object> readFileContent(Map<String, String> body) {
        String filePath = body.get("filePath");
        Map<String, Object> map = new HashMap<>();
        String fileContent = "";
        try {
            fileContent = FileUtil.readFile(filePath);
        } catch (IOException e) {
            //e.printStackTrace();
            fileContent = "没有这个文件或目录";
        }
        map.put("fileContent", fileContent);
        return map;
    }

    @Override
    public ReturnT<String> writeFileContent(Map<String, String> body) {
        String filePath = body.get("filePath");
        String content = body.get("content");
        if(filePath == null || content == null){
            return new ReturnT<>(ReturnT.FAIL_CODE, "请求体参数有误");
        }
        try {
            FileUtil.writeFile(filePath, content);
        } catch (IOException e) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "文件写入失败 "+e);
        }
        return new ReturnT<>("文件写入成功");
    }

}
