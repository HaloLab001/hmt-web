package com.wugui.hmt.admin.service.impl;

import cn.hutool.core.util.ObjectUtil;
import com.google.common.collect.Lists;
import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.admin.entity.JobDataTypeMapping;
import com.wugui.hmt.admin.entity.JobDatasource;
import com.wugui.hmt.admin.entity.JobInfo;
import com.wugui.hmt.admin.mapper.SubMetaDataMapper;
import com.wugui.hmt.admin.service.JobDataTypeMappingService;
import com.wugui.hmt.admin.service.JobDatasourceService;
import com.wugui.hmt.admin.service.SubMetaDataOracleService;
import com.wugui.hmt.admin.service.SubMetaDataService;
import com.wugui.hmt.admin.tool.meta.SubOracleDatabaseMeta;
import com.wugui.hmt.admin.tool.pojo.CreateMetaPojo;
import com.wugui.hmt.admin.tool.pojo.MetaDataPojo;
import com.wugui.hmt.admin.tool.pojo.MetaTypePojo;
import com.wugui.hmt.admin.tool.query.BaseQueryTool;
import com.wugui.hmt.admin.tool.query.QueryToolFactory;
import com.wugui.hmt.admin.util.MultiOutputStream;
import com.wugui.hmt.admin.util.SubMetaEnum;
import com.wugui.hmt.admin.util.SubMetaTypeEnum;
import com.wugui.hmt.admin.util.SubMetaUtil;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Resource;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;


import static org.apache.logging.log4j.util.Strings.isBlank;


/*-------------------------------------------------------------------------
 *
 * SubMetaDataOracleServiceImpl.java
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/service/impl/SubMetaDataOracleServiceImpl.java
 *
 *-----------------------------------------------
 */
@Transactional(rollbackFor = Exception.class)
@Service
public class SubMetaDataOracleServiceImpl implements SubMetaDataOracleService {

    protected static final Logger logger = LoggerFactory.getLogger(SubMetaDataOracleServiceImpl.class);

    @Autowired
    private JobDatasourceService jobDatasourceService;

    @Resource
    private SubMetaDataMapper subMetaDataMapper;

    @Resource
    private SubMetaDataService subMetaDataService;

    @Resource
    private JobDataTypeMappingService jobDataTypeMappingService;

    @Override
    public ReturnT<String> add(CreateMetaPojo createMetaInfo) {
        FileOutputStream propFile = null;
        MultiOutputStream multi = null;
        ReturnT<String> stringReturnT = new ReturnT<>(ReturnT.FAIL_CODE, "生成任务失败");
        try {
            String filePath = this.getClass().getResource("/").getPath()+"meta/log/" + createMetaInfo.getLogFileName() +".log";
            propFile = new FileOutputStream(filePath);
            //设置同时输出到控制台和prop文件
            multi = new MultiOutputStream(new PrintStream(propFile),System.out);
            System.setOut(new PrintStream(multi));
            //构建shell模板
            Map<String, String> shellMap = SubMetaUtil.buildText(createMetaInfo, SubMetaEnum.BUILD_TEXT_1.getTypeId(), jobDatasourceService.getById(createMetaInfo.getDatasourceId_2()), "oracle");
            //获取元数据集合
            List<MetaTypePojo> metaTypePojoList = SubMetaUtil.getMetaTypePojoList();
            for (int metaId:createMetaInfo.getMetaIds()) {
                //构建任务模板
                JobInfo jobInfo = SubMetaUtil.buildTemplateInfo(createMetaInfo, subMetaDataMapper.getJobTemplateInfoById(createMetaInfo.getTemplateId()));
                createMetaInfo.setMetaId(metaId);
                //获取元数据类型信息
                MetaTypePojo metaTypePojo = metaTypePojoList.stream().filter(m -> m.getMetaTypeId() == metaId).findFirst().get();
                //获取前置SQL语句
                String beforeSql = SubOracleDatabaseMeta.setPublicTransformParam(metaTypePojo);
                //开始构建任务
                if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.TABLE.getMetaTypeId()){
                    stringReturnT = generateTableSQL(createMetaInfo, metaTypePojo, jobInfo, beforeSql);
                } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.CONSTRAINTS.getMetaTypeId()){
                    stringReturnT = generateConstraintSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
                } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.INDEX.getMetaTypeId()){
                    stringReturnT = generateIndexSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
                } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.TRIGGER.getMetaTypeId()){
                    stringReturnT = generateTriggerSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
                } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.PACKAGE.getMetaTypeId()){
                    stringReturnT = generatePackageSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
                } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.FUNCTION.getMetaTypeId() || metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.PROCEDURE.getMetaTypeId() || metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.VIEW.getMetaTypeId() || metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.TYPE.getMetaTypeId() || metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.SYNONYM.getMetaTypeId()){
                    stringReturnT = generateFunAndProAndViewAndTypeSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
                } else{
                    stringReturnT = generatePublicSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally{
            try {
                if (multi == null){
                    return new ReturnT<>(101, "日志生成有误 java.lang.NullPointerException: null");
                }
                multi.close();
                propFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return stringReturnT;
    }

    /**
     * 生成任务
     * @param createMetaInfo
     * @param metaTypePojo
     * @param jobInfo
     * @param shellMap
     * @param beforeSql
     * @return
     */
    public ReturnT<String> generatePublicSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, Map<String, String> shellMap, String beforeSql) {
        try {
            //Oracle模式名称
            String oracleSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            //获取分页信息
            Map<String, Integer> pageMap = getPageInfo(createMetaInfo, oracleSchemaName, metaTypePojo, SubOracleDatabaseMeta.getPublicMetaCount(oracleSchemaName, metaTypePojo, createMetaInfo.getSelectAndSql()));
            int beginCount = pageMap.get("beginCount");
            int endCount = pageMap.get("endCount");
            int forIndexCount = pageMap.get("forIndexCount");
            int addCount = pageMap.get("endCount");
            for (int i = 1; i <= forIndexCount; i++) {
                //获取分页语句
                String pageSql = SubOracleDatabaseMeta.getPagePublicSqlInfo(oracleSchemaName, metaTypePojo, beginCount, endCount, createMetaInfo.getSelectAndSql());
                logger.info("当前循环次数:"+i);
                logger.info("当前处理条数:"+beginCount+","+endCount);
                List<MetaDataPojo> dataInfoList = getMetaData(createMetaInfo.getDatasourceId_1(), pageSql, beforeSql);
                for (MetaDataPojo dataInfo : dataInfoList) {
                    String sqlText = dataInfo.getSqlInfo();
                    /* 开始过滤 */
                    sqlText = filterSqlText(createMetaInfo, metaTypePojo, oracleSchemaName, haloSchemaName, dataInfo.getObjectName(), sqlText);
                    /* 结束过滤 */
                    //任务名称
                    String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + dataInfo.getObjectName();
                    //添加任务
                    subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
                }
                beginCount = beginCount + addCount;
                endCount = endCount + addCount;
            }
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成"+metaTypePojo.getMetaTypeName()+"失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成"+metaTypePojo.getMetaTypeName()+"脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * SQL语句过滤
     * @param metaTypePojo
     * @param oracleSchemaName
     * @param haloSchemaName
     * @param sqlText
     * @return
     */
    public String filterSqlText(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, String oracleSchemaName, String haloSchemaName, String objectName, String sqlText){
        /* 开始过滤 */
        //序列不需要去除双引号，有自己的处理方式
        if(metaTypePojo.getMetaTypeId() != SubMetaTypeEnum.SEQUENCE.getMetaTypeId()) {
            sqlText = sqlText
                    .replaceAll("\"" + oracleSchemaName + "\"\\.", "\"" + haloSchemaName + "\"\\.")
                    .replaceAll("\"" + haloSchemaName + "\"", haloSchemaName)
                    .replaceAll("\"" + objectName + "\"", objectName)
                    .replaceAll("OR REPLACE EDITIONABLE", "");
        }
        //单独做过滤
        if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.FUNCTION.getMetaTypeId()){
            // sqlText = sqlText
            //         .replaceAll("(?i)RAW\\(.*\\)", "RAW")
            //         .replaceAll("(?i)RAW \\(.*\\)", "RAW");
            sqlText = sqlText
                    .replaceAll("CREATE\\s+FUNCTION", "CREATE OR REPLACE FUNCTION");
        } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.PROCEDURE.getMetaTypeId()){
            sqlText = sqlText
                    .replaceAll("CREATE\\s+PROCEDURE", "CREATE OR REPLACE PROCEDURE");
        } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.VIEW.getMetaTypeId()){
            sqlText = sqlText
                    .replaceAll("\"", "")
                    .replaceAll("FORCE ", "")
                    .replaceAll("EDITIONABLE ", "")
                    .replaceAll("WITH READ ONLY", "");
            sqlText = generateComments(sqlText, createMetaInfo, oracleSchemaName, objectName);
        } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.SEQUENCE.getMetaTypeId()){
            sqlText = sqlText
                    .replaceAll(" NOORDER", "")
                    .replaceAll(" NOCACHE", "")
                    .replaceAll(" ORDER", "")
                    .replaceAll(" NOCYCLE", "")
                    .replaceAll(" NOKEEP", "")
                    .replaceAll(" NOSCALE", "")
                    .replaceAll(" GLOBAL", "")
                    .replaceAll(" NOPARTITION", "")
                    .replaceAll("\\d{18,}", "9223372036854775807");
            //处理语句中的特殊字符或中文,加上双引号并且转义
            sqlText = SubMetaUtil.disposeSqlColNameIsNormal(sqlText);
        } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.SYNONYM.getMetaTypeId()){
            sqlText = sqlText
                    .replaceAll("\"", "")
                    .replaceAll(" EDITIONABLE", "");
        }
        //去掉字符串首尾的留白
        sqlText = sqlText.trim();
        //补充末尾的;号
        if (!sqlText.substring(sqlText.length() - 1).equals(";")) {
            sqlText = sqlText + ";";
        }
        if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.TYPE.getMetaTypeId()) {
            //去除双引号
            sqlText = sqlText.replaceAll("\"", "");
            sqlText = sqlText.replaceAll("CREATE\\s+TYPE", "CREATE OR REPLACE TYPE")
                             .replaceAll("CREATE\\s+TYPE\\s+BODY", "CREATE OR REPLACE TYPE BODY");
        }
        return sqlText;
    }

    /**
     * 视图添加注释
     * @param sqlText
     * @param createMetaInfo
     * @param oracleSchemaName
     * @param objectName
     * @return
     */
    public String generateComments(String sqlText, CreateMetaPojo createMetaInfo, String oracleSchemaName, String objectName){
        //去掉字符串首尾的留白
        sqlText = sqlText.trim();
        //补充末尾的;号
        if (!sqlText.substring(sqlText.length() - 1).equals(";")) {
            sqlText = sqlText + ";";
        }
        //sqlText = "BEGIN;\n" + sqlText;
        //查询视图的注释信息
        List<Map<String, String>> tabCommentsList = getMetaDataPublic(createMetaInfo.getDatasourceId_1(), SubOracleDatabaseMeta.getAllTabComments(oracleSchemaName, objectName, SubMetaEnum.TABLE_TYPE_2.getTypeId()), SubMetaEnum.TABLE_COMMENTS_1.getTypeId());
        for (Map<String, String> map:tabCommentsList){
            String comsSql = "\nCOMMENT ON VIEW "+map.get("TABLE_NAME")+" IS '"+map.get("COMMENTS")+"';";
            sqlText = sqlText + comsSql;
        }
        //查询视图字段的注释信息
        List<Map<String, String>> tabColList = getMetaDataPublic(createMetaInfo.getDatasourceId_1(), SubOracleDatabaseMeta.getAllColCommentsList(oracleSchemaName, objectName), SubMetaEnum.TABLE_COMMENTS_2.getTypeId());
        for (Map<String, String> map:tabColList){
            String comsSql = "\nCOMMENT ON COLUMN "+map.get("TABLE_NAME")+"."+map.get("COLUMN_NAME")+" IS '"+map.get("COMMENTS")+"';";
            sqlText = sqlText + comsSql;
        }
        //sqlText = sqlText + "\nCOMMIT;";
        return sqlText;
    }

    /**
     * 生成表结构shell文本内容
     *
     * @param createMetaInfo
     * @param jobInfo
     */
    public ReturnT<String> generateTableSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, String beforeSql) {
        try {
            Map<String, String> shellMap = SubMetaUtil.buildText(createMetaInfo, SubMetaEnum.BUILD_TEXT_3.getTypeId(), jobDatasourceService.getById(createMetaInfo.getDatasourceId_2()), "oracle");
            //Oracle模式名称
            String oracleSchemaName = createMetaInfo.getSchema();
            logger.info("正在生成"+oracleSchemaName+"的表结构任务......");
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            //是否存在临时表
            boolean isTemporary = false;
            //是否存在分区表
            boolean isPartition = false;
            //查询数据类型映射的集合
            List<JobDataTypeMapping> jobDataTypeMappingList = jobDataTypeMappingService.getListBydatasource("oracle");
            //最终sql语句
            StringBuilder tableFinallySqlText = new StringBuilder();
            //最终注释语句
            StringBuilder tableCommentFinallySqlText = new StringBuilder();
            tableFinallySqlText.append("set search_path to "+haloSchemaName+",oracle,public,pg_catalog; \n");
            //获取分页信息
            Map<String, Integer> pageMap = getPageInfo(createMetaInfo, oracleSchemaName, metaTypePojo, SubOracleDatabaseMeta.getPublicMetaCount(oracleSchemaName, metaTypePojo, createMetaInfo.getSelectAndSql()));
            int beginCount = pageMap.get("beginCount");
            int endCount = pageMap.get("endCount");
            int forIndexCount = pageMap.get("forIndexCount");
            int addCount = pageMap.get("endCount");
            for (int i = 1; i <= forIndexCount; i++) {
                //获取分页语句
                String pageSql = SubOracleDatabaseMeta.getPagePublicSqlInfo(oracleSchemaName, metaTypePojo, beginCount, endCount, createMetaInfo.getSelectAndSql());
                logger.info("当前循环次数:"+i);
                logger.info("当前循环次数:"+beginCount+","+endCount);
                List<MetaDataPojo> dataInfoList = getMetaData(createMetaInfo.getDatasourceId_1(), pageSql, beforeSql);
                for (MetaDataPojo dataInfo : dataInfoList) {
                    String sqlText = dataInfo.getSqlInfo();
                    /* 开始过滤 */
                    for (JobDataTypeMapping jobDataTypeMapping:jobDataTypeMappingList) {
                        //是否保留括号中的值
                        if(jobDataTypeMapping.getIsRetainValue() == 1){
                            String readerDateType = jobDataTypeMapping.getReaderDataType();
                            //如果类型中包含括号则需要加上转义,例如 TIMESTAMP (6),否则正则替换失败
                            if(readerDateType.matches(".*[()]")){
                                readerDateType = readerDateType.replace("(", "\\(").replace(")", "\\)");
                            }
                            sqlText = sqlText.replaceAll(" "+readerDateType," "+jobDataTypeMapping.getWriterDataType());
                        }else{
                            sqlText = sqlText.replaceAll(" "+jobDataTypeMapping.getReaderDataType()+"\\(.*?\\)"," "+jobDataTypeMapping.getReaderDataType())
                                    .replaceAll(" "+jobDataTypeMapping.getReaderDataType()," "+jobDataTypeMapping.getWriterDataType());
                        }
                    }
                    sqlText = sqlText
                            .replaceAll("NO INMEMORY", "")
                            //.replaceAll(" RAW\\(.*?\\)", " RAW")
                            //将注释删除
                            .replaceAll("(/\\*)(.*?)(\\*/)", "")
                            //先将分号前的空格删除,方便后续截取
                            .replaceAll("\\s+;", ";")
                            //模式名替换
                            .replaceAll("\"" + oracleSchemaName + "\"\\.", "\"" + haloSchemaName + "\"\\.")
                            //NUMBER (*,10), *替换为38
                            .replaceAll("NUMBER\\(\\*,(.*)\\)", "NUMBER(38,$1)")
                            //FLOAT大于53的 替换 FLOAT(53)
                            .replaceAll("FLOAT\\(.*\\)", "FLOAT(53)")
                            //VARCHAR2(40 CHAR) 删除CHAR
                            .replaceAll(" CHAR(?=[^(]*\\))", "")
                            //TIMESTAMP WITH LOCAL TIME ZONE 替换 WITH TIME ZONE
                            .replaceAll("WITH LOCAL TIME ZONE", "WITH TIME ZONE")
                            //包含国家字符集数据的大型对象
                            .replaceAll(" NCLOB", " text")
                            //去掉USING INDEX 和 ENABLE 之间的字符
                            .replaceAll("(USING INDEX)(.*?)(ENABLE)", "")
                            //去掉ENABLE
                            .replaceAll(" ENABLE", "")
                            //去掉USING INDEX
                            .replaceAll("USING INDEX", "")
                            //去掉压缩索引特性
                            .replaceAll(" ORGANIZATION INDEX NOCOMPRESS", "");
                    //对临时表设置指定schema
                    if(dataInfo.getTemporary().equals("Y")){
                        sqlText = sqlText.replaceAll("\"" + haloSchemaName + "\"\\.", "\"GTT_SCHEMA\"\\.");
                    }
                    //对约束做检查
                    sqlText = sqlText.replaceAll("(?<=CHECK\\s{0,128}\\()([^\\\"].*?)(?=\\ )", "\"$1\"");
                    /* 结束过滤 */
                    //分离表结构
                    String tableSqlText = sqlText.substring(0, sqlText.indexOf(";")) + ";";//从第一位开始到) ;前结束
                    String regexc = "(?<=\")([^\\.])(.*?)(?=\")";
                    Pattern pattern = Pattern.compile(regexc);
                    Matcher matcher = pattern.matcher(tableSqlText);
                    while (matcher.find()) {
                        String tableColName = matcher.group();
                        if(!SubMetaUtil.colNameIsNormal(tableColName)){
                            if (tableColName.contains("$")) {
                                tableColName = tableColName.replace("$", "\\$");
                            }
                            tableSqlText = tableSqlText.replaceAll("\"" + tableColName + "\"", tableColName);
                        }
                    }
                    //判断是否是分区表
                    if(!StringUtils.isBlank(dataInfo.getPartitioningType())){
                        if(!isPartition){
                            isPartition = true;
                        }
                        tableSqlText = disposePartTableSQL(createMetaInfo.getDatasourceId_1(), createMetaInfo.getTablePartNameSwitch(), oracleSchemaName, dataInfo.getObjectName(), dataInfo.getSubPartitioningType(), tableSqlText);
                    }
                    tableFinallySqlText.append(tableSqlText);
                    //判断是否是临时表
                    // if(tableSqlText.toString().contains("CREATE GLOBAL TEMPORARY") && !isTemporary){
                    //     isTemporary = true;
                    // }
                    if(dataInfo.getTemporary().equals("Y") && !isTemporary){
                        isTemporary = true;
                    }
                    //查询表的注释信息
                    List<Map<String, String>> tabCommentsList = getMetaDataPublic(createMetaInfo.getDatasourceId_1(), SubOracleDatabaseMeta.getAllTabComments(oracleSchemaName, dataInfo.getObjectName(), SubMetaEnum.TABLE_TYPE_1.getTypeId()), SubMetaEnum.TABLE_COMMENTS_1.getTypeId());
                    for (Map<String, String> map:tabCommentsList){
                        String tableName = map.get("TABLE_NAME");
                        if(SubMetaUtil.colNameIsNormal(tableName)){
                            tableName = "\""+tableName+"\"";
                        }
                        String comsSql = "\n  COMMENT ON TABLE "+tableName+" IS '"+map.get("COMMENTS")+"';";
                        tableCommentFinallySqlText.append(comsSql);
                    }
                    //查询表字段的注释信息
                    List<Map<String, String>> tabColList = getMetaDataPublic(createMetaInfo.getDatasourceId_1(), SubOracleDatabaseMeta.getAllColCommentsList(oracleSchemaName, dataInfo.getObjectName()), SubMetaEnum.TABLE_COMMENTS_2.getTypeId());
                    for (Map<String, String> map:tabColList){
                        String tableName = map.get("TABLE_NAME");
                        String tableColName = map.get("COLUMN_NAME");
                        if(SubMetaUtil.colNameIsNormal(tableColName)){
                            tableColName = "\""+tableColName+"\"";
                        }
                        if(SubMetaUtil.colNameIsNormal(tableName)){
                            tableName = "\""+tableName+"\"";
                        }
                        String comsSql = "\n  COMMENT ON COLUMN "+tableName+"."+tableColName+" IS '"+map.get("COMMENTS")+"';";
                        tableCommentFinallySqlText.append(comsSql);
                    }
                }
                beginCount = beginCount + addCount;
                endCount = endCount + addCount;
            }
            //任务名称
            String jobDesc = "表结构:" + haloSchemaName;
            //增加临时表或分区表的提示
            if(isTemporary || isPartition){
                String temporaryText = isTemporary ? "临时表" : "";
                String partitionText = isPartition ? "分区表" : "";
                jobDesc = jobDesc + "(包含"+temporaryText+partitionText+")";
            }
            SimpleDateFormat dateFormat= new SimpleDateFormat("yyyyMMddhhmmss");
            //生成shell脚本返回路径
            String filePath = createFile(tableFinallySqlText.toString(), "TABLE"+dateFormat.format(new Date())+".sql", "table");
            if(filePath.equals("error")){
                throw new Exception("生成sql脚本文件失败");
            }
            jobInfo.setFilePath(filePath);
            logger.info("----------------- 表结构脚本任务生成完毕");
            logger.info("表结构SQL文件生成路径:"+filePath);
            //添加任务
            subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, SubMetaUtil.createShell(shellMap, "") + filePath, SubMetaTypeEnum.TABLE.getMetaTypeId(), SubMetaEnum.BUILD_TYPE_2.getTypeId());
            //单独添加表注释的任务
            if(!tableCommentFinallySqlText.toString().equals("")){
                String commentFilePath = createFile("set search_path to "+haloSchemaName+",oracle,public,pg_catalog; \n"+tableCommentFinallySqlText.toString(), "TABLECOMMENT"+dateFormat.format(new Date())+".sql", "table");
                jobInfo.setFilePath(commentFilePath);
                logger.info("----------------- 表注释脚本任务生成完毕");
                logger.info("表注释SQL文件生成路径:"+commentFilePath);
                jobDesc = "表注释:" + haloSchemaName;
                subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, SubMetaUtil.createShell(shellMap, "") + commentFilePath, SubMetaTypeEnum.TABLECOMMENT.getMetaTypeId(), SubMetaEnum.BUILD_TYPE_2.getTypeId());
            } else{
                logger.info("----------------- 表注释未提取到相关内容");
            }
        } catch (Exception e) {
            logger.error("生成表结构失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成表结构脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 处理分区表
     * @param schema
     * @param tableName
     * @param sqlText
     * @return
     */
    public String disposePartTableSQL(Long datasourceId, Boolean tablePartNameSwitch, String schema, String tableName, String subPartitioningType, String sqlText){
        //分区最终语句
        String partSqlText = "";
        String import_tmp = "";
        //获取主分区信息
        List<Map<String, String>> partMapList = getMetaDataPublic(datasourceId, SubOracleDatabaseMeta.getPartTablesList(schema, tableName, SubMetaEnum.PART_TABLE_1.getTypeId()), SubMetaEnum.PART_TABLE_1.getTypeId());
        List<Map<String, String>> subPartMapList = new ArrayList<>();
        //获取子分区信息
        if(!StringUtils.isBlank(subPartitioningType)){
            subPartMapList = getMetaDataPublic(datasourceId, SubOracleDatabaseMeta.getPartTablesList(schema, tableName, SubMetaEnum.PART_TABLE_2.getTypeId()), SubMetaEnum.PART_TABLE_2.getTypeId());
        }
        String old_value = "MINVALUE";
        int partitionCount = partMapList.size();
        int series = 0;
        for (Map<String, String> map:partMapList){
            //分区名称
            String PARTITION_NAME = map.get("PARTITION_NAME");
            //分区值
            String HIGH_VALUE = map.get("HIGH_VALUE");
            //表名
            String TABLE_NAME = map.get("TABLE_NAME");
            //类型
            String PARTITIONING_TYPE = map.get("PARTITIONING_TYPE");
            //新的分区名称
            String NEW_PARTITION_NAME = PARTITION_NAME;
            //是否重新生成分区名称
            if(tablePartNameSwitch){
                NEW_PARTITION_NAME = TABLE_NAME + "_P" + series;
            }
            //主分区语句
            String create_table_tmp = "CREATE TABLE "+NEW_PARTITION_NAME+" PARTITION OF "+TABLE_NAME+"\n";
            //根据分区类型变化
            Map<String, String> typeMap = SubMetaUtil.disposePartPartitioningTypeSQL(PARTITIONING_TYPE, HIGH_VALUE, old_value,  partitionCount, series++, create_table_tmp);
            create_table_tmp = typeMap.get("create_table_tmp");
            old_value = typeMap.get("new_value");
            //子分区语句
            String subPartSqlText = "";
            //处理子分区
            if(!subPartMapList.isEmpty()){
                subPartSqlText = disposeSubPartTableSQL(subPartMapList, PARTITION_NAME, tablePartNameSwitch, NEW_PARTITION_NAME);
                //有子分区的话需要去掉主分区末尾的分号
                create_table_tmp = create_table_tmp.replaceAll(";$","");
            }
            partSqlText = partSqlText + create_table_tmp + subPartSqlText;
        }
        import_tmp = "PARTITION BY "+partMapList.get(0).get("PARTITIONING_TYPE")+" ("+partMapList.get(0).get("COLUMN_NAME")+");\n";
        //删除原表的分区信息
        sqlText = sqlText.replaceAll("(?s) PARTITION BY.*", "")
                .replaceAll("\\s+$","");
        //重新拼接转换后的分区信息
        sqlText = sqlText + import_tmp + partSqlText;
        return sqlText;
    }

    /**
     * 处理子分区
     * @param subPartMapList
     * @param PARTITION_NAME
     * @return
     */
    public String disposeSubPartTableSQL(List<Map<String, String>> subPartMapList, String PARTITION_NAME, Boolean tablePartNameSwitch,String NEW_PARENT_PARTITION_NAME){
        String subPartSqlText = "";
        //处理子分区
        List<Map<String, String>> this_subpart_mapList = subPartMapList.stream().filter(m -> m.get("PARTITION_NAME").equals(PARTITION_NAME)).collect(Collectors.toList());
        String old_value = "MINVALUE";
        int partitionCount = this_subpart_mapList.size();
        int series = 0;
        for (Map<String, String> subpartMap:this_subpart_mapList){
            //子分区名称
            String SUBPARTITION_NAME = subpartMap.get("SUBPARTITION_NAME");
            //父分区名称
            String PARENT_PARTITION_NAME = subpartMap.get("PARTITION_NAME");
            //分区值
            String SUBPART_HIGH_VALUE = subpartMap.get("HIGH_VALUE");
            //类型
            String SUBPARTITIONING_TYPE = subpartMap.get("SUBPARTITIONING_TYPE");
            //是否重新生成子分区名称
            if(tablePartNameSwitch){
                SUBPARTITION_NAME = NEW_PARENT_PARTITION_NAME + "_P" + series;
            }
            String subpart_create_table_tmp = "CREATE TABLE "+SUBPARTITION_NAME+" PARTITION OF "+NEW_PARENT_PARTITION_NAME+"\n";
            //根据分区类型变化
            Map<String, String> typeMap = SubMetaUtil.disposePartPartitioningTypeSQL(SUBPARTITIONING_TYPE, SUBPART_HIGH_VALUE, old_value,  partitionCount, series++, subpart_create_table_tmp);
            subpart_create_table_tmp = typeMap.get("create_table_tmp");
            old_value = typeMap.get("new_value");
            subPartSqlText = subPartSqlText + subpart_create_table_tmp;
        }
        String subpart_import_tmp = "PARTITION BY "+this_subpart_mapList.get(0).get("SUBPARTITIONING_TYPE")+" ("+this_subpart_mapList.get(0).get("COLUMN_NAME")+");\n";
        subPartSqlText = subpart_import_tmp + subPartSqlText +"\n";
        return subPartSqlText;
    }


    /**
     * 生成约束任务
     * @param createMetaInfo
     * @param metaTypePojo
     * @param jobInfo
     * @param shellMap
     * @param beforeSql
     * @return
     */
    public ReturnT<String> generateConstraintSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, Map<String, String> shellMap, String beforeSql) {
        try {
            //Oracle模式名称
            String oracleSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            //获取分页信息
            Map<String, Integer> pageMap = getPageInfo(createMetaInfo, oracleSchemaName, metaTypePojo, SubOracleDatabaseMeta.getMetaConstraintsTypeCCount(oracleSchemaName));
            int beginCount = pageMap.get("beginCount");
            int endCount = pageMap.get("endCount");
            int forIndexCount = pageMap.get("forIndexCount");
            int addCount = pageMap.get("endCount");
            for (int i = 1; i <= forIndexCount; i++) {
                //获取分页语句
                String pageSql = SubOracleDatabaseMeta.getConstraintsTypeCList(oracleSchemaName, beginCount, endCount);
                logger.info("当前循环次数:"+i);
                logger.info("当前处理条数:"+beginCount+","+endCount);
                List<Map<String, String>> dataInfoList = getMetaDataPublic(createMetaInfo.getDatasourceId_1(), pageSql, SubMetaEnum.CONSTRAINT_TYPE_1.getTypeId());
                for (Map<String, String> map : dataInfoList) {
                    String sqlText = "";
                    String owner = map.get("OWNER");
                    String tableName = map.get("TABLE_NAME");
                    String constraintName = map.get("CONSTRAINT_NAME");
                    String searchCondition = map.get("SEARCH_CONDITION");
                    String temporary = map.get("TEMPORARY");
                    //处理约束名称
                    constraintName = disposeConstraintName(constraintName, tableName);
                    //对临时表做schema指定
                    if(temporary.equals("Y")){
                        owner = "GTT_SCHEMA";
                    }
                    sqlText = "ALTER TABLE \""+owner+"\".\""+tableName+"\" ADD CONSTRAINT \""+constraintName+"\" CHECK ("+searchCondition+");";
                    //处理语句中的特殊字符或中文,加上双引号并且转义
                    sqlText = SubMetaUtil.disposeSqlColNameIsNormal(sqlText);
                    //去除双引号
                    //sqlText = sqlText.replaceAll("\"", "");
                    //任务名称
                    String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + constraintName;
                    //添加任务
                    subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
                }
                beginCount = beginCount + addCount;
                endCount = endCount + addCount;
            }
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成完毕");
            logger.info("----------------- 开始生成主键约束脚本任务");
            // constraint_type P 主键处理
            List<Map<String, String>> typePList = getMetaDataPublic(createMetaInfo.getDatasourceId_1(), SubOracleDatabaseMeta.getConstraintsTypePList(oracleSchemaName), SubMetaEnum.CONSTRAINT_TYPE_2.getTypeId());
            // 根据约束名称分组
            Map<String, List<Map<String, String>>> groupByP_Map = typePList.stream().collect(Collectors.groupingBy(doc -> doc.get("CONSTRAINT_NAME")));
            groupByP_Map.forEach((k, v) -> {
                String A_OWNER = "";
                String A_TABLE_NAME = "";
                String A_CONSTRAINT_NAME = "";
                String A_FIELD = "";
                String A_TEMPORARY = "";
                for (Map<String, String> map : v){
                    String OWNER = map.get("OWNER");
                    String TABLE_NAME = map.get("TABLE_NAME");
                    String CONSTRAINT_NAME = map.get("CONSTRAINT_NAME");
                    String COLUMN_NAME = map.get("COLUMN_NAME");
                    String TEMPORARY = map.get("TEMPORARY");
                    A_OWNER = OWNER;
                    A_TABLE_NAME = TABLE_NAME;
                    A_CONSTRAINT_NAME = CONSTRAINT_NAME;
                    A_FIELD = A_FIELD + "\"" + COLUMN_NAME + "\",";
                    A_TEMPORARY = TEMPORARY;
                }
                //处理约束名称
                A_CONSTRAINT_NAME = disposeConstraintName(A_CONSTRAINT_NAME, A_TABLE_NAME);
                //对临时表做schema指定
                if(A_TEMPORARY.equals("Y")){
                    A_OWNER = "GTT_SCHEMA";
                }
                String sqlText = "ALTER TABLE \""+A_OWNER+"\".\""+A_TABLE_NAME+"\" ADD CONSTRAINT \""+A_CONSTRAINT_NAME+"\" PRIMARY KEY ("+A_FIELD.substring(0, A_FIELD.length()-1)+");";
                //处理语句中的特殊字符或中文,加上双引号并且转义
                sqlText = SubMetaUtil.disposeSqlColNameIsNormal(sqlText);
                //任务名称
                String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + A_CONSTRAINT_NAME;
                //添加任务
                subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
            });
            logger.info("----------------- 主键约束脚本任务生成完毕");
            logger.info("----------------- 开始生成外键约束脚本任务");
            // constraint_type R 外键处理
            List<Map<String, String>> typeRList = getMetaDataPublic(createMetaInfo.getDatasourceId_1(), SubOracleDatabaseMeta.getConstraintsTypeRList(oracleSchemaName), SubMetaEnum.CONSTRAINT_TYPE_3.getTypeId());
            // 根据约束名称分组
            Map<String, List<Map<String, String>>> groupByR_Map = typeRList.stream().collect(Collectors.groupingBy(doc -> doc.get("CONSTRAINT_NAME")));
            groupByR_Map.forEach((k, v) -> {
                String A_OWNER = "";
                String B_OWNER = "";
                String A_TABLE_NAME = "";
                String B_TABLE_NAME = "";
                String A_CONSTRAINT_NAME = "";
                String A_FIELD = "";
                String B_FIELD = "";
                for (Map<String, String> map : v){
                    String OWNER = map.get("OWNER");
                    String TABLE_NAME = map.get("TABLE_NAME");
                    String CONSTRAINT_NAME = map.get("CONSTRAINT_NAME");
                    String COLUMN_NAME = map.get("COLUMN_NAME");
                    String R_OWNER = map.get("R_OWNER");
                    String R_TABLE_NAME = map.get("R_TABLE_NAME");
                    String R_COLUMN_NAME = map.get("R_COLUMN_NAME");
                    A_OWNER = OWNER;
                    B_OWNER = R_OWNER;
                    A_TABLE_NAME = TABLE_NAME;
                    B_TABLE_NAME = R_TABLE_NAME;
                    A_CONSTRAINT_NAME = CONSTRAINT_NAME;
                    A_FIELD = A_FIELD + "\"" + COLUMN_NAME + "\",";
                    B_FIELD = B_FIELD + "\"" + R_COLUMN_NAME + "\",";
                }
                //处理约束名称
                A_CONSTRAINT_NAME = disposeConstraintName(A_CONSTRAINT_NAME, A_TABLE_NAME);
                String sqlText = "ALTER TABLE \""+A_OWNER+"\".\""+A_TABLE_NAME+"\" ADD CONSTRAINT \""+A_CONSTRAINT_NAME+"\" FOREIGN KEY ("+A_FIELD.substring(0, A_FIELD.length()-1)+")" +
                        " REFERENCES \""+B_OWNER+"\".\""+B_TABLE_NAME+"\" ("+B_FIELD.substring(0, B_FIELD.length()-1)+") ;";
                //处理语句中的特殊字符或中文,加上双引号并且转义
                sqlText = SubMetaUtil.disposeSqlColNameIsNormal(sqlText);
                //任务名称
                String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + A_CONSTRAINT_NAME;
                //添加任务
                subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
            });
            logger.info("----------------- 外键约束脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成"+metaTypePojo.getMetaTypeName()+"失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成"+metaTypePojo.getMetaTypeName()+"脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 生成表索引shell文本内容
     *
     * @param createMetaInfo
     * @param jobInfo
     * @param shellMap
     * @param beforeSql
     */
    public ReturnT<String> generateIndexSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, Map<String, String> shellMap, String beforeSql) {
        try {
            //查询出约束中建立过的索引名称
            List<MetaDataPojo> indexInfoList = getMetaData(createMetaInfo.getDatasourceId_1(), SubOracleDatabaseMeta.getUserConstraintsIndexList(createMetaInfo.getSchema()), null);
            //拼接成字符串
            String indexText = indexInfoList.stream().map(MetaDataPojo::getSqlInfo).collect(Collectors.joining(";"));
            //Oracle模式名称
            String oracleSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            //获取分页信息
            Map<String, Integer> pageMap = getPageInfo(createMetaInfo, oracleSchemaName, metaTypePojo, SubOracleDatabaseMeta.getPublicMetaCount(oracleSchemaName, metaTypePojo, createMetaInfo.getSelectAndSql()));
            int beginCount = pageMap.get("beginCount");
            int endCount = pageMap.get("endCount");
            int forIndexCount = pageMap.get("forIndexCount");
            int addCount = pageMap.get("endCount");
            for (int i = 1; i <= forIndexCount; i++) {
                //获取分页语句
                String pageSql = SubOracleDatabaseMeta.getPagePublicSqlInfo(oracleSchemaName, metaTypePojo, beginCount, endCount, createMetaInfo.getSelectAndSql());
                logger.info("当前循环次数:"+i);
                logger.info("当前循环次数:"+beginCount+","+endCount);
                List<MetaDataPojo> dataInfoList = getMetaData(createMetaInfo.getDatasourceId_1(), pageSql, beforeSql);
                for (MetaDataPojo dataInfo : dataInfoList) {
                    String sqlText = dataInfo.getSqlInfo();
                    /* 开始过滤 */
                    sqlText = sqlText
                            //模式名替换
                            .replaceAll("\"" + oracleSchemaName + "\"\\.", "\"" + haloSchemaName + "\"\\.")
                            //去掉USING INDEX 和 ENABLE 之间的字符
                            .replaceAll("(USING INDEX)(.*?)(ENABLE)", "")
                            //去掉ENABLE
                            .replaceAll(" ENABLE", "")
                            //去掉USING INDEX
                            .replaceAll("USING INDEX", "")
                            //去掉BITMAP
                            .replaceAll(" BITMAP", "")
                            //去除多余的回车
                            .replaceAll("\n", "")
                            //去除多余的空格
                            .replaceAll("\\s+;", ";");
                    //去除双引号
                    //.replaceAll("\"", "");
                    //删除脏数据(生成的时候会有建立索引不全的语句)
                    if (sqlText.contains("(;")) {
                        continue;
                    }
                    //在)后面的LOCAL及其所有后续内容被删除
                    sqlText = sqlText.replaceAll("\\)\\s*LOCAL.*", ");");
                    //获取主键约束名称
                    String updateSqlText = sqlText.replaceAll("\"", "");
                    String cName = cutString(updateSqlText, "CREATE UNIQUE INDEX " + oracleSchemaName + ".", " ON");
                    //过滤重复约束名称的语句
                    if (indexText.contains(cName)) {
                        continue;
                    }
                    //对临时表设置指定schema
                    if(dataInfo.getTemporary().equals("Y")){
                        sqlText = sqlText.replaceAll("\"" + haloSchemaName + "\"\\.", "\"GTT_SCHEMA\"\\.");
                    }
                    //处理语句中的特殊字符或中文,加上双引号并且转义
                    sqlText = SubMetaUtil.disposeSqlColNameIsNormal(sqlText);
                    //临时处理特殊字符
                    sqlText = sqlText.replace("$", "\\$"); 
                    /* 结束过滤 */
                    //任务名称
                    String jobDesc = "索引:" + haloSchemaName + "." + dataInfo.getObjectName();
                    //添加任务
                    if (!StringUtils.isBlank(sqlText)) {
                        subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
                    }
                }
                beginCount = beginCount + addCount;
                endCount = endCount + addCount;
            }
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成索引失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成索引脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 生成函数、存储过程、视图、自定义类型脚本
     * @param createMetaInfo
     * @param metaTypePojo
     * @param jobInfo
     * @param shellMap
     * @param beforeSql
     * @return
     */
    public ReturnT<String> generateFunAndProAndViewAndTypeSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, Map<String, String> shellMap, String beforeSql) {
        //shellMap = SubMetaUtil.buildText(createMetaInfo, SubMetaEnum.BUILD_TEXT_5.getTypeId(), jobDatasourceService.getById(createMetaInfo.getDatasourceId_2()), "oracle");
        shellMap = SubMetaUtil.buildText(createMetaInfo, SubMetaEnum.BUILD_TEXT_4.getTypeId(), jobDatasourceService.getById(createMetaInfo.getDatasourceId_2()), "oracle");
        try {
            //Oracle模式名称
            String oracleSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            //获取分页信息
            Map<String, Integer> pageMap = getPageInfo(createMetaInfo, oracleSchemaName, metaTypePojo, SubOracleDatabaseMeta.getPublicMetaCount(oracleSchemaName, metaTypePojo, createMetaInfo.getSelectAndSql()));
            int beginCount = pageMap.get("beginCount");
            int endCount = pageMap.get("endCount");
            int forIndexCount = pageMap.get("forIndexCount");
            int addCount = pageMap.get("endCount");
            String beginText = "set search_path to "+haloSchemaName+",oracle,public,pg_catalog;\n";
            for (int i = 1; i <= forIndexCount; i++) {
                //获取分页语句
                String pageSql = SubOracleDatabaseMeta.getPagePublicSqlInfo(oracleSchemaName, metaTypePojo, beginCount, endCount, createMetaInfo.getSelectAndSql());
                logger.info("当前循环次数:"+i);
                logger.info("当前处理条数:"+beginCount+","+endCount);
                List<MetaDataPojo> dataInfoList = getMetaData(createMetaInfo.getDatasourceId_1(), pageSql, beforeSql);
                for (MetaDataPojo dataInfo : dataInfoList) {
                    String sqlText = dataInfo.getSqlInfo();
                    /* 开始过滤 */
                    sqlText = filterSqlText(createMetaInfo, metaTypePojo, oracleSchemaName, haloSchemaName, dataInfo.getObjectName(), sqlText);
                    sqlText = beginText + sqlText;
                    /* 结束过滤 */
                    //生成shell脚本返回路径
                    String filePath = createFile(sqlText, haloSchemaName+"-"+dataInfo.getObjectName()+".sql", metaTypePojo.getObjectType().toLowerCase());
                    if(filePath.equals("error")){
                        throw new Exception("生成sql脚本文件失败");
                    }
                    jobInfo.setFilePath(filePath);
                    sqlText = filePath;
                    //任务名称
                    String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + dataInfo.getObjectName();
                    //添加任务
                    subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
                }
                beginCount = beginCount + addCount;
                endCount = endCount + addCount;
            }
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成"+metaTypePojo.getMetaTypeName()+"失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成"+metaTypePojo.getMetaTypeName()+"脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 生成触发器shell文本内容
     * @param createMetaInfo
     * @param metaTypePojo
     * @param jobInfo
     * @param shellMap
     * @param beforeSql
     * @return
     */
    public ReturnT<String> generateTriggerSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, Map<String, String> shellMap, String beforeSql) {
        shellMap = SubMetaUtil.buildText(createMetaInfo, SubMetaEnum.BUILD_TEXT_5.getTypeId(), jobDatasourceService.getById(createMetaInfo.getDatasourceId_2()), "oracle");
        try {
            //Oracle模式名称
            String oracleSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            //获取分页信息
            Map<String, Integer> pageMap = getPageInfo(createMetaInfo, oracleSchemaName, metaTypePojo, SubOracleDatabaseMeta.getMetaTriggerCount(oracleSchemaName));
            int beginCount = pageMap.get("beginCount");
            int endCount = pageMap.get("endCount");
            int forIndexCount = pageMap.get("forIndexCount");
            int addCount = pageMap.get("endCount");
            String beginText = "set search_path to "+haloSchemaName+",oracle,public,pg_catalog;\n";
            for (int i = 1; i <= forIndexCount; i++) {
                //获取分页语句
                String pageSql = SubOracleDatabaseMeta.getPageTriggerSqlInfo(oracleSchemaName, beginCount, endCount);
                logger.info("当前循环次数:"+i);
                logger.info("当前处理条数:"+beginCount+","+endCount);
                List<Map<String, String>> dataInfoList = getMetaDataPublic(createMetaInfo.getDatasourceId_1(), pageSql, SubMetaEnum.TRIGGER_TYPE_1.getTypeId());
                for (Map<String, String> map : dataInfoList) {
                    String sqlText = "";
                    String TRIGGER_NAME = map.get("TRIGGER_NAME");
                    String TRIGGER_BODY = map.get("TRIGGER_BODY");
                    String TABLE_NAME = map.get("TABLE_NAME");
                    String TRIGGERING_EVENT = map.get("TRIGGERING_EVENT");
                    String TRIGGER_TYPE = map.get("TRIGGER_TYPE");
                    String DESCRIPTION = map.get("DESCRIPTION");
                    String WHEN_CLAUSE = map.get("WHEN_CLAUSE");
                    String TRIGGER_TYPE_NAME = "";
                    // 触发条件
                    String WHEN_INFO = "";
                    // 级别操作
                    String TRIGGER_LEVEL = "";
                    //触发器函数存在则删除
                    String function_drop = "DROP FUNCTION IF EXISTS "+TRIGGER_NAME+"_FUNCTION CASCADE;\n";
                    // 建立触发器函数
                    String function_head = "CREATE FUNCTION "+TRIGGER_NAME+"_FUNCTION() " +
                            "\nRETURNS trigger AS" +
                            "\n$$\n";
                    String function_body = TRIGGER_BODY;
                    function_body = function_body.replaceAll("(?i):NEW.", " NEW.")
                            .replaceAll("(?i):OLD.", " OLD.");
                    // 对返回值做处理,NEW 只出现在insert和update时,OLD只出现在update和delete时,update都会有,此处暂只对delete做处理
                    String function_return = "RETURN NEW;";
                    if(function_body.equals("DELETE") || function_body.equals("delete")){
                        function_return = "RETURN OLD;";
                    }
                    function_body = function_body.replaceAll("(?i)end "+TRIGGER_NAME+";", "end;")
                            .replaceAll("(?i)end;", ""+function_return+" end;")
                            .replaceAll("(?i)end\\s+;", ""+function_return+" end;")
                            .replaceAll("(?i)((IF)|(ELSIF)) INSERTING", "$1 (TG_OP = 'INSERT')")
                            .replaceAll("(?i)((IF)|(ELSIF)) UPDATING", "$1 (TG_OP = 'UPDATE')")
                            .replaceAll("(?i)((IF)|(ELSIF)) DELETING", "$1 (TG_OP = 'DELETE')");
                    String function_bottom = "\n$$\nLANGUAGE \"plpgsql\";";
                    String final_sql = function_drop + function_head + function_body + function_bottom;
                    // 区分BEFORE和AFTER
                    if(TRIGGER_TYPE.equals("BEFORE EACH ROW")){
                        TRIGGER_TYPE_NAME = "BEFORE";
                    } else if(TRIGGER_TYPE.equals("AFTER EACH ROW")){
                        TRIGGER_TYPE_NAME = "AFTER";
                    }
                    // 区分语句级触发器与行级触发器
                    if(DESCRIPTION.toUpperCase().contains("FOR EACH ROW")){
                        TRIGGER_LEVEL = "\nFOR EACH ROW";
                    } else{
                        TRIGGER_LEVEL = "\nFOR STATEMENT";
                    }
                    // 指定触发条件
                    if(WHEN_CLAUSE != null && !WHEN_CLAUSE.equals("")){
                        WHEN_INFO = "\nWHEN ("+WHEN_CLAUSE+") ";
                    }
                    //表名校验
                    if(SubMetaUtil.colNameIsNormal(TABLE_NAME)){
                        TABLE_NAME = "\""+TABLE_NAME+"\"";
                    }
                    //触发器存在则删除
                    String trigger_drop = "\nDROP TRIGGER IF EXISTS "+TRIGGER_NAME+"\n" +
                            "ON "+TABLE_NAME+" CASCADE;\n";
                    // 建立触发器
                    String trigger_sql = "CREATE TRIGGER "+TRIGGER_NAME+" " +
                            "\n"+TRIGGER_TYPE_NAME+" "+TRIGGERING_EVENT+" " +
                            "\nON "+TABLE_NAME+" " +
                            TRIGGER_LEVEL +
                            WHEN_INFO +
                            "\nEXECUTE FUNCTION "+TRIGGER_NAME+"_FUNCTION();";
                    sqlText = beginText + final_sql + "\n" + trigger_drop + trigger_sql;
                    //生成shell脚本返回路径
                    String filePath = createFile(sqlText, haloSchemaName+"-"+TRIGGER_NAME+".sql", metaTypePojo.getObjectType().toLowerCase());
                    if(filePath.equals("error")){
                        throw new Exception("生成sql脚本文件失败");
                    }
                    jobInfo.setFilePath(filePath);
                    sqlText = filePath;
                    //任务名称
                    String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + TRIGGER_NAME;
                    //添加任务
                    subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
                }
                beginCount = beginCount + addCount;
                endCount = endCount + addCount;
            }
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成"+metaTypePojo.getMetaTypeName()+"失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成"+metaTypePojo.getMetaTypeName()+"脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 生成package任务
     * @param createMetaInfo
     * @param metaTypePojo
     * @param jobInfo
     * @param shellMap
     * @param beforeSql
     * @return
     */
    public ReturnT<String> generatePackageSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, Map<String, String> shellMap, String beforeSql) {
        shellMap = SubMetaUtil.buildText(createMetaInfo, SubMetaEnum.BUILD_TEXT_4.getTypeId(), jobDatasourceService.getById(createMetaInfo.getDatasourceId_2()), "oracle");
        try {
            //Oracle模式名称
            String oracleSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            //获取分页信息
            Map<String, Integer> pageMap = getPageInfo(createMetaInfo, oracleSchemaName, metaTypePojo, SubOracleDatabaseMeta.getPublicMetaCount(oracleSchemaName, metaTypePojo, createMetaInfo.getSelectAndSql()));
            int beginCount = pageMap.get("beginCount");
            int endCount = pageMap.get("endCount");
            int forIndexCount = pageMap.get("forIndexCount");
            int addCount = pageMap.get("endCount");
            String beginText = "set search_path to "+haloSchemaName+",oracle,public,pg_catalog;\n";
            for (int i = 1; i <= forIndexCount; i++) {
                //获取分页语句
                String pageSql = SubOracleDatabaseMeta.getPagePublicSqlInfo(oracleSchemaName, metaTypePojo, beginCount, endCount, createMetaInfo.getSelectAndSql());
                logger.info("当前循环次数:"+i);
                logger.info("当前处理条数:"+beginCount+","+endCount);
                List<MetaDataPojo> dataInfoList = getMetaData(createMetaInfo.getDatasourceId_1(), pageSql, beforeSql);
                for (MetaDataPojo dataInfo : dataInfoList) {
                    String sqlText = dataInfo.getSqlInfo();
                    /* 开始过滤 */
                    sqlText = filterSqlText(createMetaInfo, metaTypePojo, oracleSchemaName, haloSchemaName, dataInfo.getObjectName(), sqlText);
                    sqlText = beginText + sqlText
                                .replaceAll("CREATE OR REPLACE PACKAGE BODY", "/\nCREATE OR REPLACE PACKAGE BODY")
                                .replaceAll("CREATE\\s+PACKAGE\\s+BODY","/\nCREATE OR REPLACE PACKAGE BODY")
                                + "\n/";
                    sqlText = sqlText.replaceAll("CREATE\\s+PACKAGE", "CREATE OR REPLACE PACKAGE");
                    //生成shell脚本返回路径
                    //idworker.nextId() 本来采用唯一id,怕出现错误找文件比较难,暂用函数名称
                    String filePath = createFile(sqlText, haloSchemaName+"-"+dataInfo.getObjectName()+".sql", "package");
                    if(filePath.equals("error")){
                        throw new Exception("生成sql脚本文件失败");
                    }
                    jobInfo.setFilePath(filePath);
                    sqlText = filePath;
                    /* 结束过滤 */
                    //任务名称
                    String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + dataInfo.getObjectName();
                    //添加任务
                    subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
                }
                beginCount = beginCount + addCount;
                endCount = endCount + addCount;
            }
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成"+metaTypePojo.getMetaTypeName()+"失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成"+metaTypePojo.getMetaTypeName()+"脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 获取分页信息
     * @return
     */
    public Map<String, Integer> getPageInfo(CreateMetaPojo createMetaInfo, String oracleSchemaName, MetaTypePojo metaTypePojo, String sql) throws IOException {
        Map<String, Integer> objectMap = new HashMap<>();
        int allCount = getMetaDataCount(createMetaInfo.getDatasourceId_1(), sql);
        int beginCount = 1;
        int endCount = 100;
        // 向上取整
        int forIndexCount = (int)Math.ceil(Double.parseDouble(String.valueOf(allCount)) / endCount);
        logger.info("----------------- 开始生成"+metaTypePojo.getMetaTypeName()+"脚本任务");
        logger.info("总任务数:"+allCount);
        logger.info("需要循环的次数:"+forIndexCount);
        objectMap.put("allCount", allCount);
        objectMap.put("beginCount", beginCount);
        objectMap.put("endCount", endCount);
        objectMap.put("forIndexCount", forIndexCount);
        return objectMap;
    }

    /**
     * 获取数据
     * @param id
     * @param executeSql 查询语句
     * @param beforeSql 在查询语句之前 执行的语句
     * @return
     * @throws IOException
     */
    public List<MetaDataPojo> getMetaData(Long id, String executeSql, String beforeSql) {
        //获取数据源对象
        JobDatasource datasource = jobDatasourceService.getById(id);
        //queryTool组装
        if (ObjectUtil.isNull(datasource)) {
            return Lists.newArrayList();
        }
        //判断数据源
        if(!datasource.getDatasource().equals("oracle")){
            // 此处借用此异常抛出
            throw new UnsupportedOperationException("暂只支持Oracle");
        }
        BaseQueryTool qTool = QueryToolFactory.getByDbType(datasource);
        List<MetaDataPojo> metaDataPojoList = new ArrayList<>();
        if(StringUtils.isBlank(beforeSql)){
            metaDataPojoList = qTool.getMetaDataSQLs(executeSql);
        } else{
            metaDataPojoList = qTool.getMetaDataSQLs(executeSql, beforeSql);
        }
        return metaDataPojoList;
    }


    public List<Map<String, String>> getMetaDataPublic(Long id, String executeSql, int type) {
        //获取数据源对象
        JobDatasource datasource = jobDatasourceService.getById(id);
        //queryTool组装
        if (ObjectUtil.isNull(datasource)) {
            return Lists.newArrayList();
        }
        //判断数据源
//        if(!datasource.getDatasource().equals("oracle")){
//            // 此处借用此异常抛出
//            throw new UnsupportedOperationException("暂只支持Oracle");
//        }
        BaseQueryTool qTool = QueryToolFactory.getByDbType(datasource);
        List<Map<String, String>> metaDataPojoList = qTool.getMetaPublicSQLs(executeSql, type);
        return metaDataPojoList;
    }

    /**
     * 统计
     * @param id
     * @param executeSql
     * @return
     * @throws IOException
     */
    public int getMetaDataCount(Long id, String executeSql) throws IOException {
        //获取数据源对象
        JobDatasource datasource = jobDatasourceService.getById(id);
        //判断数据源
        if(!datasource.getDatasource().equals("oracle")){
            // 此处借用此异常抛出
            throw new UnsupportedOperationException("暂只支持Oracle");
        }
        BaseQueryTool qTool = QueryToolFactory.getByDbType(datasource);
        return qTool.getMetaDataCountSQLs(executeSql);
    }

    /**
     * 生成约束语句(用于与Halo约束语句进行对比)
     * @param oracleSchemaName
     * @param datasourceId
     * @return
     */
    @Override
    public List<Map<String, String>> generateCompareConstraintSQL(String oracleSchemaName, Long datasourceId) {
        List<Map<String, String>> constraintList = new ArrayList<>();
        try {
            List<Map<String, String>> dataInfoList = getMetaDataPublic(datasourceId, SubOracleDatabaseMeta.getConstraintsTypeCListNotPage(oracleSchemaName), SubMetaEnum.CONSTRAINT_TYPE_1.getTypeId());
            for (Map<String, String> map : dataInfoList) {
                String sqlText = "";
                String owner = map.get("OWNER");
                String tableName = map.get("TABLE_NAME");
                String constraintName = map.get("CONSTRAINT_NAME");
                String searchCondition = map.get("SEARCH_CONDITION");
                sqlText = "ALTER TABLE \""+owner+"\".\""+tableName+"\" ADD CONSTRAINT \""+constraintName+"\" CHECK ("+searchCondition+");";
                //处理语句中的特殊字符或中文,加上双引号并且转义
                sqlText = SubMetaUtil.disposeSqlColNameIsNormal(sqlText);
                Map<String, String> objMap = new HashMap<>();
                objMap.put("CONSTRAINT_NAME", constraintName);
                objMap.put("SQL", sqlText);
                constraintList.add(objMap);
            }
            // constraint_type P 主键处理
            List<Map<String, String>> typePList = getMetaDataPublic(datasourceId, SubOracleDatabaseMeta.getConstraintsTypePList(oracleSchemaName), SubMetaEnum.CONSTRAINT_TYPE_2.getTypeId());
            // 根据约束名称分组
            Map<String, List<Map<String, String>>> groupByP_Map = typePList.stream().collect(Collectors.groupingBy(doc -> doc.get("CONSTRAINT_NAME")));
            groupByP_Map.forEach((k, v) -> {
                String A_OWNER = "";
                String A_TABLE_NAME = "";
                String A_CONSTRAINT_NAME = "";
                String A_FIELD = "";
                for (Map<String, String> map : v){
                    String OWNER = map.get("OWNER");
                    String TABLE_NAME = map.get("TABLE_NAME");
                    String CONSTRAINT_NAME = map.get("CONSTRAINT_NAME");
                    String COLUMN_NAME = map.get("COLUMN_NAME");
                    A_OWNER = OWNER;
                    A_TABLE_NAME = TABLE_NAME;
                    A_CONSTRAINT_NAME = CONSTRAINT_NAME;
                    A_FIELD = A_FIELD + "\"" + COLUMN_NAME + "\",";
                }
                String sqlText = "ALTER TABLE \""+A_OWNER+"\".\""+A_TABLE_NAME+"\" ADD CONSTRAINT \""+A_CONSTRAINT_NAME+"\" PRIMARY KEY ("+A_FIELD.substring(0, A_FIELD.length()-1)+");";
                //处理语句中的特殊字符或中文,加上双引号并且转义
                sqlText = SubMetaUtil.disposeSqlColNameIsNormal(sqlText);
                Map<String, String> objMap = new HashMap<>();
                objMap.put("CONSTRAINT_NAME", A_CONSTRAINT_NAME);
                objMap.put("SQL", sqlText);
                constraintList.add(objMap);
            });
            // constraint_type R 外键处理
            List<Map<String, String>> typeRList = getMetaDataPublic(datasourceId, SubOracleDatabaseMeta.getConstraintsTypeRList(oracleSchemaName), SubMetaEnum.CONSTRAINT_TYPE_3.getTypeId());
            // 根据约束名称分组
            Map<String, List<Map<String, String>>> groupByR_Map = typeRList.stream().collect(Collectors.groupingBy(doc -> doc.get("CONSTRAINT_NAME")));
            groupByR_Map.forEach((k, v) -> {
                String A_OWNER = "";
                String B_OWNER = "";
                String A_TABLE_NAME = "";
                String B_TABLE_NAME = "";
                String A_CONSTRAINT_NAME = "";
                String A_FIELD = "";
                String B_FIELD = "";
                for (Map<String, String> map : v){
                    String OWNER = map.get("OWNER");
                    String TABLE_NAME = map.get("TABLE_NAME");
                    String CONSTRAINT_NAME = map.get("CONSTRAINT_NAME");
                    String COLUMN_NAME = map.get("COLUMN_NAME");
                    String R_OWNER = map.get("R_OWNER");
                    String R_TABLE_NAME = map.get("R_TABLE_NAME");
                    String R_COLUMN_NAME = map.get("R_COLUMN_NAME");
                    A_OWNER = OWNER;
                    B_OWNER = R_OWNER;
                    A_TABLE_NAME = TABLE_NAME;
                    B_TABLE_NAME = R_TABLE_NAME;
                    A_CONSTRAINT_NAME = CONSTRAINT_NAME;
                    A_FIELD = A_FIELD + "\"" + COLUMN_NAME + "\",";
                    B_FIELD = B_FIELD + "\"" + R_COLUMN_NAME + "\",";
                }
                String sqlText = "ALTER TABLE \""+A_OWNER+"\".\""+A_TABLE_NAME+"\" ADD CONSTRAINT \""+A_CONSTRAINT_NAME+"\" FOREIGN KEY ("+A_FIELD.substring(0, A_FIELD.length()-1)+")" +
                        " REFERENCES \""+B_OWNER+"\".\""+B_TABLE_NAME+"\" ("+B_FIELD.substring(0, B_FIELD.length()-1)+") ;";
                //处理语句中的特殊字符或中文,加上双引号并且转义
                sqlText = SubMetaUtil.disposeSqlColNameIsNormal(sqlText);
                Map<String, String> objMap = new HashMap<>();
                objMap.put("CONSTRAINT_NAME", A_CONSTRAINT_NAME);
                objMap.put("SQL", sqlText);
                constraintList.add(objMap);
            });
        } catch (Exception e) {
            logger.error("生成约束语句失败:",e);
        }
        return constraintList;
    }

    /**
     * 生成索引语句(用于与Halo索引语句进行对比)
     * @param oracleSchemaName
     * @param datasourceId
     * @return
     */
    @Override
    public List<Map<String, String>> generateCompareIndexSQL(String oracleSchemaName, Long datasourceId) {
        List<Map<String, String>> constraintList = new ArrayList<>();
        try {
            MetaTypePojo metaTypePojo = new MetaTypePojo();
            metaTypePojo.setObjectType("INDEX");
            metaTypePojo.setMetaTypeId(4);
            //获取前置SQL语句
            String beforeSql = SubOracleDatabaseMeta.setPublicTransformParam(metaTypePojo);
            //获取分页信息
            Map<String, Integer> pageMap = getComparePageInfo(datasourceId, "索引", SubOracleDatabaseMeta.getPublicMetaCount(oracleSchemaName, metaTypePojo, ""));
            int beginCount = pageMap.get("beginCount");
            int endCount = pageMap.get("endCount");
            int forIndexCount = pageMap.get("forIndexCount");
            int addCount = pageMap.get("endCount");
            for (int i = 1; i <= forIndexCount; i++) {
                //获取分页语句
                String pageSql = SubOracleDatabaseMeta.getPagePublicSqlInfo(oracleSchemaName, metaTypePojo, beginCount, endCount, "");
                logger.info("当前循环次数:"+i);
                logger.info("当前循环次数:"+beginCount+","+endCount);
                List<MetaDataPojo> dataInfoList = getMetaData(datasourceId, pageSql, beforeSql);
                for (MetaDataPojo dataInfo : dataInfoList) {
                    String sqlText = dataInfo.getSqlInfo();
                    /* 开始过滤 */
                    sqlText = sqlText
                            //去掉USING INDEX 和 ENABLE 之间的字符
                            .replaceAll("(USING INDEX)(.*?)(ENABLE)", "")
                            //去掉ENABLE
                            .replaceAll(" ENABLE", "")
                            //去掉USING INDEX
                            .replaceAll("USING INDEX", "")
                            //去掉BITMAP
                            .replaceAll(" BITMAP", "")
                            //去除多余的回车
                            .replaceAll("\n", "")
                            //去除多余的空格
                            .replaceAll("\\s+;", ";");
                    //删除脏数据(生成的时候会有建立索引不全的语句)
                    if (sqlText.contains("(;")) {
                        continue;
                    }
                    //处理语句中的特殊字符或中文,加上双引号并且转义
                    // sqlText = SubMetaUtil.disposeSqlColNameIsNormal(sqlText);
                    // // disposeSqlColNameIsNormal中有将剩余的双引号替换为转义形式的方式，所以这里做临时处理
                    // sqlText = sqlText.replace("\\", "");
                    /* 结束过滤 */
                    Map<String, String> objMap = new HashMap<>();
                    objMap.put("INDEX_NAME", dataInfo.getObjectName());
                    objMap.put("SQL", sqlText);
                    constraintList.add(objMap);
                }
                beginCount = beginCount + addCount;
                endCount = endCount + addCount;
            }
            logger.info("----------------- 索引脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成索引失败:",e);
        }
        return constraintList;
    }

    @Override
    public List<Map<String, String>> generateComparePublicSQL(String oracleSchemaName, Long datasourceId, int metaTypeId) {
        List<Map<String, String>> constraintList = new ArrayList<>();
        try {
            MetaTypePojo metaTypePojo = new MetaTypePojo();
            String typeName = "";
            if(metaTypeId == SubMetaTypeEnum.FUNCTION.getMetaTypeId()) {
                metaTypePojo.setObjectType(SubMetaTypeEnum.FUNCTION.getObjectType());
                metaTypePojo.setMetaTypeId(SubMetaTypeEnum.FUNCTION.getMetaTypeId());
                typeName = SubMetaTypeEnum.FUNCTION.getMetaTypeName();
            } else if(metaTypeId == SubMetaTypeEnum.PROCEDURE.getMetaTypeId()){
                metaTypePojo.setObjectType(SubMetaTypeEnum.PROCEDURE.getObjectType());
                metaTypePojo.setMetaTypeId(SubMetaTypeEnum.PROCEDURE.getMetaTypeId());
                typeName = SubMetaTypeEnum.PROCEDURE.getMetaTypeName();
            } else if(metaTypeId == SubMetaTypeEnum.VIEW.getMetaTypeId()){
                metaTypePojo.setObjectType(SubMetaTypeEnum.VIEW.getObjectType());
                metaTypePojo.setMetaTypeId(SubMetaTypeEnum.VIEW.getMetaTypeId());
                typeName = SubMetaTypeEnum.VIEW.getMetaTypeName();
            } else if(metaTypeId == SubMetaTypeEnum.SEQUENCE.getMetaTypeId()){
                metaTypePojo.setObjectType(SubMetaTypeEnum.SEQUENCE.getObjectType());
                metaTypePojo.setMetaTypeId(SubMetaTypeEnum.SEQUENCE.getMetaTypeId());
                typeName = SubMetaTypeEnum.SEQUENCE.getMetaTypeName();
            } else if(metaTypeId == SubMetaTypeEnum.TRIGGER.getMetaTypeId()){
                metaTypePojo.setObjectType(SubMetaTypeEnum.TRIGGER.getObjectType());
                metaTypePojo.setMetaTypeId(SubMetaTypeEnum.TRIGGER.getMetaTypeId());
                typeName = SubMetaTypeEnum.TRIGGER.getMetaTypeName();
            } else if(metaTypeId == SubMetaTypeEnum.TYPE.getMetaTypeId()){
                metaTypePojo.setObjectType(SubMetaTypeEnum.TYPE.getObjectType());
                metaTypePojo.setMetaTypeId(SubMetaTypeEnum.TYPE.getMetaTypeId());
                typeName = SubMetaTypeEnum.TYPE.getMetaTypeName();
            } else if(metaTypeId == SubMetaTypeEnum.PACKAGE.getMetaTypeId()){
                metaTypePojo.setObjectType(SubMetaTypeEnum.PACKAGE.getObjectType());
                metaTypePojo.setMetaTypeId(SubMetaTypeEnum.PACKAGE.getMetaTypeId());
                typeName = SubMetaTypeEnum.PACKAGE.getMetaTypeName();
            }
            //获取前置SQL语句
            String beforeSql = SubOracleDatabaseMeta.setPublicTransformParam(metaTypePojo);
            //获取分页信息
            Map<String, Integer> pageMap = getComparePageInfo(datasourceId, typeName, SubOracleDatabaseMeta.getPublicMetaCount(oracleSchemaName, metaTypePojo, ""));
            int beginCount = pageMap.get("beginCount");
            int endCount = pageMap.get("endCount");
            int forIndexCount = pageMap.get("forIndexCount");
            int addCount = pageMap.get("endCount");
            for (int i = 1; i <= forIndexCount; i++) {
                //获取分页语句
                String pageSql = SubOracleDatabaseMeta.getPagePublicSqlInfo(oracleSchemaName, metaTypePojo, beginCount, endCount, "");
                logger.info("当前循环次数:"+i);
                logger.info("当前处理条数:"+beginCount+","+endCount);
                List<MetaDataPojo> dataInfoList = getMetaData(datasourceId, pageSql, beforeSql);
                for (MetaDataPojo dataInfo : dataInfoList) {
                    String sqlText = dataInfo.getSqlInfo();
                    /* 开始过滤 */
                    //去掉字符串首尾的留白
                    sqlText = sqlText.trim();
                    if(metaTypeId == SubMetaTypeEnum.SEQUENCE.getMetaTypeId()){
                        //补充末尾的;号
                        if (!sqlText.substring(sqlText.length() - 1).equals(";")) {
                            sqlText = sqlText + ";";
                        }
                    }
                    //sqlText = filterSqlText(createMetaInfo, metaTypePojo, oracleSchemaName, haloSchemaName, dataInfo.getObjectName(), sqlText);
                    /* 结束过滤 */
                    Map<String, String> objMap = new HashMap<>();
                    objMap.put("OBJECT_NAME", dataInfo.getObjectName());
                    objMap.put("SQL", sqlText);
                    constraintList.add(objMap);
                }
                beginCount = beginCount + addCount;
                endCount = endCount + addCount;
            }
            logger.info("----------------- "+typeName+"脚本任务生成完毕");
        } catch (Exception e) {
            //logger.error("生成"+metaTypePojo.getMetaTypeName()+"失败:",e);
        }
        return constraintList;
    }

    public Map<String, Integer> getComparePageInfo(Long datasourceId, String typeName, String sql) throws IOException {
        Map<String, Integer> objectMap = new HashMap<>();
        int allCount = getMetaDataCount(datasourceId, sql);
        int beginCount = 1;
        int endCount = 100;
        // 向上取整
        int forIndexCount = (int)Math.ceil(Double.parseDouble(String.valueOf(allCount)) / endCount);
        logger.info("----------------- 开始生成"+typeName+"对比任务");
        logger.info("总任务数:"+allCount);
        logger.info("需要循环的次数:"+forIndexCount);
        objectMap.put("allCount", allCount);
        objectMap.put("beginCount", beginCount);
        objectMap.put("endCount", endCount);
        objectMap.put("forIndexCount", forIndexCount);
        return objectMap;
    }

    /**
     * 处理约束名称
     * @param constraintName
     * @param tableName
     * @return
     */
    public String disposeConstraintName(String constraintName, String tableName){
        //Oracle的约束名称虽然不能重复,但是允许和表名相同。因为Halo是不允许相同的,防止约束名称和表名相同,所以需要做转换。
        if(constraintName.equals(tableName)){
            constraintName = constraintName + "_HALO";
        }
        //约束名称中如果包含$,在Linux中会当作系统变量,此处需要做转义
        constraintName = constraintName.replace("$", "\\$");
        return constraintName;
    }

    /**
     * 生成文件
     * @param content
     */
    public String createFile(String content, String fileName, String folderName){
        String filePath = "";
        FileWriter fw = null;
        try
        {
            String path = this.getClass().getResource("/").getPath()+"meta/";
            if(!folderName.equals("")){
                // 创建File对象
                File directory = new File(path + folderName);
                // 如果文件夹不存在，则创建文件夹
                if (!directory.exists()) {
                    directory.mkdirs();
                }
                path = path + folderName + "/";
            }
            filePath = path + fileName;
            File file = new File(filePath);
            if (!file.exists())
            {
                file.createNewFile();
            }
            fw = new FileWriter(filePath);
            fw.write(content);
            return filePath;
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            try
            {
                fw.close();
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }
        return "error";
    }

    /**
     * 字符串截取中间部分
     */
    public static String cutString(String str, String start, String end) {
        try {
            if (isBlank(str)) {
                return str;
            }
            String reg = start + "(.*)" + end;
            Pattern pattern = Pattern.compile(reg);
            Matcher matcher = pattern.matcher(str);
            while (matcher.find()) {
                str = matcher.group(1);
            }
        } catch (Exception e) {

        }
        return str;
    }



}
