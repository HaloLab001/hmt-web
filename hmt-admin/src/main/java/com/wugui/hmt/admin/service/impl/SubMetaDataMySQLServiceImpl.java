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
import com.wugui.hmt.admin.service.SubMetaDataMySQLService;
import com.wugui.hmt.admin.service.SubMetaDataService;
import com.wugui.hmt.admin.tool.meta.SubMySQLDatabaseMeta;
import com.wugui.hmt.admin.tool.meta.SubOracleDatabaseMeta;
import com.wugui.hmt.admin.tool.pojo.CreateMetaPojo;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.interceptor.TransactionAspectSupport;

/*-------------------------------------------------------------------------
 *
 * SubMetaDataMySQLServiceImpl.java
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/service/impl/SubMetaDataMySQLServiceImpl.java
 *
 *-----------------------------------------------
 */
@Transactional(rollbackFor = Exception.class)
@Service
public class SubMetaDataMySQLServiceImpl implements SubMetaDataMySQLService {

    protected static final Logger logger = LoggerFactory.getLogger(SubMetaDataMySQLServiceImpl.class);

    @Resource
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
            Map<String, String> shellMap = SubMetaUtil.buildText(createMetaInfo, SubMetaEnum.BUILD_TEXT_1.getTypeId(), jobDatasourceService.getById(createMetaInfo.getDatasourceId_2()), "mysql");
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
                //切换至MySQL命令
                if(createMetaInfo.getMysqlCommandSwitch() == true){
                    shellMap = SubMetaUtil.buildMySQLText(createMetaInfo, "-c");
                }
                //开始构建任务
                if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.TABLE.getMetaTypeId()){
                    stringReturnT = generateTableSQL(createMetaInfo, metaTypePojo, jobInfo, beforeSql);
                } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.CONSTRAINTS.getMetaTypeId()){
                    stringReturnT = generateConstraintSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
                    stringReturnT = generateCheckConstraintSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
                } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.INDEX.getMetaTypeId()){
                    stringReturnT = generateIndexSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
                } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.FUNCTION.getMetaTypeId()){
                    stringReturnT = generateFunctionSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
                } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.PROCEDURE.getMetaTypeId()){
                    stringReturnT = generateProcedureSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
                } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.VIEW.getMetaTypeId()){
                    stringReturnT = generateViewSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
                } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.SEQUENCE.getMetaTypeId()){
                    stringReturnT = generateSequenceSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
                } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.TRIGGER.getMetaTypeId()){
                    stringReturnT = generateTriggerSQL(createMetaInfo, metaTypePojo, jobInfo, shellMap, beforeSql);
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
     * 生成表结构shell文本内容
     * @param createMetaInfo
     * @param jobInfo
     */
    public ReturnT<String> generateTableSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, String beforeSql) {
        try {
            Map<String, String> shellMap = SubMetaUtil.buildText(createMetaInfo, SubMetaEnum.BUILD_TEXT_3.getTypeId(), jobDatasourceService.getById(createMetaInfo.getDatasourceId_2()), "mysql");
            //MySQL模式名称
            String mysqlSchemaName = createMetaInfo.getSchema();
            logger.info("正在生成"+mysqlSchemaName+"的表结构任务......");
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            //是否存在临时表
            boolean isTemporary = false;
            //是否存在分区表
            boolean isPartition = false;
            //查询数据类型映射的集合
            List<JobDataTypeMapping> jobDataTypeMappingList = jobDataTypeMappingService.getListBydatasource("mysql");
            //最终sql语句
            StringBuilder tableFinallySqlText = new StringBuilder();
            //最终注释语句
            StringBuilder tableCommentFinallySqlText = new StringBuilder();
            if(createMetaInfo.getMysqlCommandSwitch() == false) {
                tableFinallySqlText.append("set search_path to "+haloSchemaName+",mysql,public; \n");
            }
            List<Map<String, String>> dataInfoList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getTableList(mysqlSchemaName));
            logger.info("----------------- 表结构任务生成中...");
            for (Map<String, String> tableMap : dataInfoList) {
                String sqlText = "";
                //表名
                String tableName = tableMap.get("TABLE_NAME");
                //表注释
                String tableComment = tableMap.get("TABLE_COMMENT");
                //是否分区表
                String createOptions = tableMap.get("CREATE_OPTIONS");
                //列注释
                StringBuilder tableColumnsComment = new StringBuilder();
                //MySQL加上反单引号
                if(createMetaInfo.getMysqlCommandSwitch() == true) {
                    tableName = "`" + tableName + "`";
                }
                //生成建表语句
                StringBuilder sb = new StringBuilder();
                sb.append("CREATE TABLE ");
                sb.append(tableName);
                sb.append("(");
                //获取列信息
                List<Map<String, String>> columnsList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getColumnsListByTable(mysqlSchemaName, tableName.replaceAll("`", "")));
                for (Map<String, String> map:columnsList) {
                    String COLUMN_NAME = map.get("COLUMN_NAME");
                    String COLUMN_TYPE = map.get("COLUMN_TYPE");
                    String COLUMN_COMMENT = map.get("COLUMN_COMMENT");
                    //校验列名
                    if(createMetaInfo.getTableAndColSwitch() == true){
                        if(SubMetaUtil.colNameIsNormal(COLUMN_NAME)){
                            COLUMN_NAME = "\"" + COLUMN_NAME + "\"";
                        }
                    }
                    //MySQL加上反单引号
                    if(createMetaInfo.getMysqlCommandSwitch() == true) {
                        COLUMN_NAME = "`" + COLUMN_NAME + "`";
                    }
                    sb.append("\n").append("  \b"); //此处加\b只是为了以防字段名被当作数据类型转换掉
                    sb.append(COLUMN_NAME);
                    sb.append(" ");
                    sb.append(COLUMN_TYPE);
                    sb.append(",");
                    if(!COLUMN_COMMENT.equals("")) {
                        //单引号需要连续两个单引号进行转义
                        if(COLUMN_COMMENT.contains("'")){
                            COLUMN_COMMENT = COLUMN_COMMENT.replaceAll("'", "''");
                        }
                        if(createMetaInfo.getMysqlCommandSwitch() == false) {
                            // psql
                            tableColumnsComment.append("\nCOMMENT ON COLUMN ").append(tableName).append(".").append(COLUMN_NAME).append(" IS '").append(COLUMN_COMMENT).append("';");
                        } else{
                            // mysql
                            tableColumnsComment.append("\nALTER TABLE ").append(tableName).append(" MODIFY COLUMN ").append(COLUMN_NAME).append(" ").append(COLUMN_TYPE).append(" COMMENT '").append(COLUMN_COMMENT).append("';");
                        }
                    }
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append("\n").append(");");
                sqlText = sb.toString();
                //如果是MySQL命令则不需要进行类型转换
                //false 继续按原psql执行，true 按mysql执行
                if(createMetaInfo.getMysqlCommandSwitch() == false) {
                    //数据类型映射
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
                } else{
                    //切换至MySQL命令
                    shellMap = SubMetaUtil.buildMySQLText(createMetaInfo, "-f");
                }
                //把\b去除
                sqlText = sqlText.replaceAll("\b", "");
                //判断是否是分区表 目前遇到了partitioned以及row_format=DYNAMIC partitioned,所以此处使用contains
                if(createOptions.contains("partitioned")){
                    if(!isPartition){
                        isPartition = true;
                    }
                    sqlText = sqlText.substring(0, sqlText.length() - 1);
                    //false: 以psql的语法生成分区 true: 以MySQL的语法生成分区,从show create table里面截取
                    if(createMetaInfo.getMysqlCommandSwitch() == false){
                        sqlText = sqlText + " " + disposePartTableSQL(createMetaInfo.getDatasourceId_1(), mysqlSchemaName, tableName);
                    } else{
                        //通过show create table获取到分区信息
                        List<Map<String, String>> parList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getTableInfoByName(mysqlSchemaName, tableName.replaceAll("`", "")));
                        sqlText = sqlText + " " + extractPartitionStatement(parList.get(0).get("Create Table")) + ";";
                    }
                }
                String tableCommentSql = "";
                //生成注释
                if(!tableComment.equals("")){
                    //单引号需要连续两个单引号进行转义
                    if(tableComment.contains("'")){
                        tableComment = tableComment.replaceAll("'", "''");
                    }
                    if(createMetaInfo.getMysqlCommandSwitch() == false) {
                        //psql
                        tableCommentSql = "\nCOMMENT ON TABLE "+tableName+" IS '"+tableComment+"';";
                    } else{
                        //mysql
                        tableCommentSql = "\nALTER TABLE "+tableName+" COMMENT = '"+tableComment+"';";
                    }
                }
                tableFinallySqlText.append(sqlText).append("\n\n");
                tableCommentFinallySqlText.append(tableCommentSql).append(tableColumnsComment.toString());
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
            if(createMetaInfo.getMysqlCommandSwitch() == false){
                //添加任务
                subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, SubMetaUtil.createShell(shellMap, "") + filePath, SubMetaTypeEnum.TABLE.getMetaTypeId(), SubMetaEnum.BUILD_TYPE_2.getTypeId());
            } else{
                subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, filePath, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
            }
            //单独添加表注释的任务
            logger.info("----------------- 表注释脚本任务生成中...");
            if(!tableCommentFinallySqlText.toString().equals("")){
                String commentFilePath = "";
                if(createMetaInfo.getMysqlCommandSwitch() == false){ 
                    commentFilePath = createFile("set search_path to "+haloSchemaName+",mysql,public; \n"+tableCommentFinallySqlText.toString(), "TABLECOMMENT"+dateFormat.format(new Date())+".sql", "table");
                } else{
                    commentFilePath = createFile(tableCommentFinallySqlText.toString(), "TABLECOMMENT"+dateFormat.format(new Date())+".sql", "table");
                }
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
     * 获取主分区信息
     * @param datasourceId
     * @param schema
     * @param tableName
     * @return
     */
    public String disposePartTableSQL(Long datasourceId, String schema, String tableName){
        Boolean tablePartNameSwitch = true;
        //分区最终语句
        String partSqlText = "";
        String import_tmp = "";
        //查询分区信息
        List<Map<String, String>> partMapList = getMetaDataUnification(datasourceId, SubMySQLDatabaseMeta.getPartMapListByTable(schema, tableName));
        //按分区名称过滤掉子分区的内容
        partMapList = partMapList .stream().collect(
                Collectors.collectingAndThen(Collectors.toCollection(() -> new TreeSet<>(Comparator.comparing(o -> o.get("PARTITION_NAME") ))),
                        ArrayList::new)
        );
        String old_value = "MINVALUE";
        int partitionCount = partMapList.size();
        int series = 0;
        for (Map<String, String> map:partMapList){
            //分区名称
            String PARTITION_NAME = map.get("PARTITION_NAME");
            //分区值
            String HIGH_VALUE = map.get("PARTITION_DESCRIPTION");
            //表名
            String TABLE_NAME = map.get("TABLE_NAME");
            //类型
            String PARTITIONING_TYPE = map.get("PARTITION_METHOD");
            String SUBPARTITION_METHOD = map.get("SUBPARTITION_METHOD");
            //新的分区名称
            String NEW_PARTITION_NAME = PARTITION_NAME;
            //是否重新生成分区名称
            if(tablePartNameSwitch){
                //NEW_PARTITION_NAME = TABLE_NAME + "_P" + series;
                NEW_PARTITION_NAME = TABLE_NAME + "_" + NEW_PARTITION_NAME;
            }
            //主分区语句
            String create_table_tmp = "CREATE TABLE "+NEW_PARTITION_NAME+" PARTITION OF "+TABLE_NAME+"\n";
            //根据分区类型变化
            Map<String, String> typeMap = disposePartPartitioningTypeSQL(PARTITIONING_TYPE, HIGH_VALUE, old_value,  partitionCount, series++, create_table_tmp);
            create_table_tmp = typeMap.get("create_table_tmp");
            old_value = typeMap.get("new_value");
            //子分区语句
            String subPartSqlText = "";
            //处理子分区
            if(SUBPARTITION_METHOD != null){
                subPartSqlText = disposeSubPartTableSQL(partMapList, PARTITION_NAME, tablePartNameSwitch, NEW_PARTITION_NAME);
                //有子分区的话需要去掉主分区末尾的分号
                create_table_tmp = create_table_tmp.replaceAll(";$","");
            }
            partSqlText = partSqlText + create_table_tmp + subPartSqlText;
        }
        String PARTITION_METHOD = partMapList.get(0).get("PARTITION_METHOD");
        //将KEY分区转换为HASH分区
        if(PARTITION_METHOD.equals("KEY")){
            PARTITION_METHOD = "HASH";
        }
        import_tmp = "PARTITION BY "+PARTITION_METHOD+" ("+partMapList.get(0).get("PARTITION_EXPRESSION").replaceAll("`", "")+");\n";
        return import_tmp + partSqlText;
    }

    /**
     * 根据MySQL文档表示，子分区可以使用 HASH或KEY分区。这也称为复合分区。
     * 所以语法只需要处理HASH和KEY分区。
     * @return
     */
    public String disposeSubPartTableSQL(List<Map<String, String>> subPartMapList, String PARTITION_NAME, Boolean tablePartNameSwitch, String NEW_PARENT_PARTITION_NAME){
        String subPartSqlText = "";
        String old_value = "MINVALUE";
        int partitionCount = subPartMapList.size();
        int series = 0;
        for (Map<String, String> subpartMap:subPartMapList){
            //子分区名称
            String SUBPARTITION_NAME = subpartMap.get("SUBPARTITION_NAME");
            //父分区名称
            String PARENT_PARTITION_NAME = subpartMap.get("PARTITION_NAME");
            //分区值
            String SUBPART_HIGH_VALUE = "";
            //类型
            String SUBPARTITIONING_TYPE = subpartMap.get("SUBPARTITION_METHOD");
            //表名
            String TABLE_NAME = subpartMap.get("TABLE_NAME");
            //是否重新生成子分区名称
            if(tablePartNameSwitch){
                //SUBPARTITION_NAME = NEW_PARENT_PARTITION_NAME + "_P" + series;
                SUBPARTITION_NAME = TABLE_NAME + "_" + SUBPARTITION_NAME;
            }
            String subpart_create_table_tmp = "CREATE TABLE "+SUBPARTITION_NAME+" PARTITION OF "+NEW_PARENT_PARTITION_NAME+"\n";
            //根据分区类型变化
            Map<String, String> typeMap = disposePartPartitioningTypeSQL(SUBPARTITIONING_TYPE, SUBPART_HIGH_VALUE, old_value,  partitionCount, series++, subpart_create_table_tmp);
            subpart_create_table_tmp = typeMap.get("create_table_tmp");
            old_value = typeMap.get("new_value");
            subPartSqlText = subPartSqlText + subpart_create_table_tmp;
        }
        String subpart_import_tmp = "PARTITION BY "+subPartMapList.get(0).get("SUBPARTITION_METHOD")+" ("+subPartMapList.get(0).get("SUBPARTITION_EXPRESSION").replaceAll("`", "")+");\n";
        subPartSqlText = subpart_import_tmp + subPartSqlText +"\n";
        return subPartSqlText;
    }

    /**
     * 从CREATE TABLE语句中提取分区语句。
     * @param createTableSql 包含分区语句的CREATE TABLE SQL语句
     */
    public static String extractPartitionStatement(String createTableSql) {
        //正则表达式匹配以"/*!"开始，包含"PARTITION BY"关键字，并以"*/"结束的分区语句
        //注意：.*? 表示非贪婪匹配，以防止匹配到最后一个"*/"之前的所有内容
        String partitionRegex = "\\/\\*!.*?PARTITION BY.*?\\*\\/";
        //使用Pattern和Matcher来查找匹配项
        java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(partitionRegex, java.util.regex.Pattern.DOTALL);
        java.util.regex.Matcher matcher = pattern.matcher(createTableSql);
        if (matcher.find()) {
            //提取匹配的分区语句（包括注释标记）
            String partitionStatementWithComments = matcher.group();
            //去除前后的注释标记（如果它们存在）
            String partitionStatement = partitionStatementWithComments.substring("/*!".length(), partitionStatementWithComments.length() - "*/".length()).trim();
            //删除第一次出现的PARTITION BY前面的所有字符
            partitionStatement = partitionStatement.substring(partitionStatement.indexOf("PARTITION BY"));
            return partitionStatement;
        }
        //如果没有找到分区语句，则返回空字符串
        return "";
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
            //MySQL模式名称
            String mysqlSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            logger.info("----------------- 开始生成列属性中的非空约束、默认值约束");
            //获取所有列属性
            List<Map<String, String>> dataInfoList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getColumnsConstraintList(mysqlSchemaName));
            for (Map<String, String> tableMap : dataInfoList) {
                String sqlText = "";
                //表名
                String tableName = tableMap.get("TABLE_NAME");
                //列名
                String columnName = tableMap.get("COLUMN_NAME");
                //校验列名
                if(createMetaInfo.getTableAndColSwitch() == true){
                    if(SubMetaUtil.colNameIsNormal(columnName)){
                        columnName = "\\\"" + columnName + "\\\"";
                    }
                }
                //是否可空
                String isNullable = tableMap.get("IS_NULLABLE");
                //默认值
                String columnDefault = tableMap.get("COLUMN_DEFAULT");
                //列类型
                String COLUMN_TYPE = tableMap.get("COLUMN_TYPE");
                //生成非空约束语句
                if(isNullable.equals("NO")){
                    if(createMetaInfo.getMysqlCommandSwitch() == false) {
                        sqlText = "ALTER TABLE "+haloSchemaName+"."+tableName+" ALTER COLUMN "+columnName+" SET NOT NULL;";
                    } else{
                        sqlText = "ALTER TABLE \\`"+tableName+"\\` MODIFY \\`"+columnName+"\\` "+COLUMN_TYPE+" NOT NULL;";
                    }
                    //任务名称
                    String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + tableName + "." + columnName + "_NOTNULL";
                    //添加任务
                    subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
                }
                //生成默认值约束语句
                if(columnDefault != null){
                    if(!columnDefault.contains("CURRENT_TIMESTAMP")){
                        columnDefault = "'" +columnDefault+ "'";
                    } else{
                        //这里是对TIMESTAMP默认值做处理 EXTRA中是 on update CURRENT_TIMESTAMP(3)
                        columnDefault = columnDefault + " " + tableMap.get("EXTRA").replaceAll("DEFAULT_GENERATED", "");
                    }
                    if(createMetaInfo.getMysqlCommandSwitch() == false) {
                        sqlText = "ALTER TABLE "+haloSchemaName+"."+tableName+" ALTER COLUMN "+columnName+" SET DEFAULT "+columnDefault+";";
                    } else{
                        sqlText = "ALTER TABLE \\`"+tableName+"\\` ALTER \\`"+columnName+"\\` SET DEFAULT "+columnDefault+";";
                        //对默认值CURRENT_TIMESTAMP做语法处理
                        if (columnDefault.contains("CURRENT_TIMESTAMP")) {
                            //使用MODIFY需要将非空重新设置
                            if(isNullable.equals("NO")){
                                sqlText = "ALTER TABLE \\`"+tableName+"\\` MODIFY \\`"+columnName+"\\` "+COLUMN_TYPE+" NOT NULL DEFAULT "+columnDefault+";";
                            } else{
                                sqlText = "ALTER TABLE \\`"+tableName+"\\` MODIFY \\`"+columnName+"\\` "+COLUMN_TYPE+" DEFAULT "+columnDefault+";";
                            }
                        }
                    }
                    //任务名称
                    String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + tableName + "." + columnName + "_DEFAULT";
                    //添加任务
                    subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
                }
            }
            logger.info("----------------- 列属性中的非空约束、默认值约束生成完毕");
            logger.info("----------------- 开始生成主键约束");
            //获取所有的主键(P)
            List<Map<String, String>> typePList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getColumnsConstraintTypePList(mysqlSchemaName));
            //根据表名分组,拼接表中的PRIMARY KEY
            Map<String, List<Map<String, String>>> groupByP_Map = typePList.stream().collect(Collectors.groupingBy(doc -> doc.get("TABLE_NAME")));
            groupByP_Map.forEach((k, v) -> {
                String aTableName = "";
                String aFieLd = "";
                for (Map<String, String> map : v){
                    String tableName = map.get("TABLE_NAME");
                    String columnName = map.get("COLUMN_NAME");
                    //校验列名
                    if(createMetaInfo.getTableAndColSwitch() == true){
                        if(SubMetaUtil.colNameIsNormal(columnName)){
                            columnName = "\\\"" + columnName + "\\\"";
                        }
                    }
                    //MySQL增加反单引号
                    if(createMetaInfo.getMysqlCommandSwitch() == true) {
                        columnName = "\\`" + columnName + "\\`";
                    }
                    aTableName = tableName;
                    aFieLd = aFieLd + columnName + ",";
                }
                String sqlText = "";
                if(createMetaInfo.getMysqlCommandSwitch() == false) {
                    sqlText = "ALTER TABLE "+haloSchemaName+"."+aTableName+" ADD PRIMARY KEY ("+aFieLd.substring(0, aFieLd.length()-1)+");";
                } else{
                    sqlText = "ALTER TABLE \\`"+aTableName+"\\` ADD PRIMARY KEY ("+aFieLd.substring(0, aFieLd.length()-1)+");";
                }
                //任务名称
                String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + aTableName + "_PKEY";
                //添加任务
                subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
            });
            logger.info("----------------- 主键约束生成完毕");
        } catch (Exception e) {
            logger.error("生成"+metaTypePojo.getMetaTypeName()+"失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成"+metaTypePojo.getMetaTypeName()+"脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 生成Check约束任务
     * @param createMetaInfo
     * @param metaTypePojo
     * @param jobInfo
     * @param shellMap
     * @param beforeSql
     * @return
     */
    public ReturnT<String> generateCheckConstraintSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, Map<String, String> shellMap, String beforeSql) {
        try {
            //MySQL模式名称
            String mysqlSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            //获取存在Check约束的表集合
            List<Map<String, String>> dataList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getCheckConstraintList(mysqlSchemaName));
            logger.info("----------------- Check约束脚本任务生成中...");
            for (Map<String, String> dataInfo : dataList) {
                //表名
                String tableName = dataInfo.get("TABLE_NAME");
                //通过show create table获取到Check约束
                List<Map<String, String>> checkList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getTableInfoByName(mysqlSchemaName, tableName));
                for (Map<String, String> checkInfo:checkList) {
                    String createText = checkInfo.get("Create Table");
                    Pattern pattern = Pattern.compile("CHECK (.*)");
                    Matcher matcher = pattern.matcher(createText);
                    while (matcher.find()) {
                        //定义语句
                        String checkText = matcher.group(0);
                        String sqlText = "";
                        //false: psql true:mysql
                        if(createMetaInfo.getMysqlCommandSwitch() == false) {
                            checkText = checkText
                                .replaceAll("`", "")
                                //检查末尾是否有逗号,有则替换为空字符串
                                .replaceFirst(",$", "");
                            sqlText = "ALTER TABLE "+haloSchemaName+"."+tableName+" ADD "+checkText+";";
                        } else{
                            checkText = checkText.replaceFirst(",$", "");
                            sqlText = "ALTER TABLE \\`"+tableName+"\\` ADD "+checkText+";";
                        }
                        //任务名称
                        String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + tableName + ".CHECK";
                        //添加任务
                        subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
                    }
                }
            }
            logger.info("----------------- Check约束脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成"+metaTypePojo.getMetaTypeName()+"失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成"+metaTypePojo.getMetaTypeName()+"脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 生成索引shell文本内容
     * @param createMetaInfo
     * @param jobInfo
     * @param shellMap
     * @param beforeSql
     */
    public ReturnT<String> generateIndexSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, Map<String, String> shellMap, String beforeSql) {
        try {
            //MySQL模式名称
            String mysqlSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            //获取索引集合,该语句会过滤掉主键唯一索引,因为主键约束执行之后会默认创建主键唯一索引
            List<Map<String, String>> indexList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getIndexList(mysqlSchemaName));
            /*
              MySQL中，索引名称在同一个表中必须是唯一的，不能重复。
             */
            final int[] indexNumber = {100000};
            //根据表名分组
            final Map<String, List<Map<String, String>>>[] groupByTableMap = new Map[]{indexList.stream().collect(Collectors.groupingBy(doc -> doc.get("TABLE_NAME")))};
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成中...");
            groupByTableMap[0].forEach((tableMapK, tableMapV) -> {
                //同一个表中再根据索引名称分组
                Map<String, List<Map<String, String>>> groupByIndexNameMap = tableMapV.stream().collect(Collectors.groupingBy(doc -> doc.get("INDEX_NAME")));
                groupByIndexNameMap.forEach((indexNameMapK, indexNameMapV) -> {
                    //表名
                    String tableName = indexNameMapV.get(0).get("TABLE_NAME");
                    //索引名称
                    String indexName = indexNameMapV.get(0).get("INDEX_NAME");
                    //是否唯一: 唯一0 不唯一1
                    String nonUnique = indexNameMapV.get(0).get("NON_UNIQUE");
                    //字段
                    String fields = "";
                    for (Map<String, String> map : indexNameMapV) {
                        //列名
                        String columnName = map.get("COLUMN_NAME");
                        //校验列名
                        if(createMetaInfo.getTableAndColSwitch() == true){
                            if(SubMetaUtil.colNameIsNormal(columnName)){
                                columnName = "\\\"" + columnName + "\\\"";
                            }
                        }
                        //MySQL增加反单引号
                        if(createMetaInfo.getMysqlCommandSwitch() == true) {
                            columnName = "\\`" + columnName + "\\`";
                        }
                        fields = fields + columnName + ",";
                    }
                    String indexNewName = indexName;
                    //psql在末尾拼接唯一编号,解决重复索引名称问题,而以MySQL命令迁移不需要处理,内核那边会处理
                    if(createMetaInfo.getMysqlCommandSwitch() == false) {
                        indexNewName = indexName+"_"+ indexNumber[0];
                    }
                    //最终sql语句
                    String sqlText = "";
                    if(nonUnique.equals("0")){
                        if(createMetaInfo.getMysqlCommandSwitch() == false) {
                            sqlText = "CREATE UNIQUE INDEX "+indexNewName+" ON "+haloSchemaName+"."+tableName+" ("+fields.substring(0, fields.length()-1)+");";
                        } else{
                            sqlText = "CREATE UNIQUE INDEX \\`"+indexNewName+"\\` ON \\`"+tableName+"\\` ("+fields.substring(0, fields.length()-1)+");";
                        }
                    } else {
                        if(createMetaInfo.getMysqlCommandSwitch() == false) {
                            sqlText = "CREATE INDEX "+indexNewName+" ON "+haloSchemaName+"."+tableName+" ("+fields.substring(0, fields.length()-1)+");";
                        } else{
                            sqlText = "CREATE INDEX \\`"+indexNewName+"\\` ON \\`"+tableName+"\\` ("+fields.substring(0, fields.length()-1)+");";
                        }
                    }
                    //任务名称
                    String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + tableName + "." + indexNewName;
                    //添加任务
                    subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
                    indexNumber[0] = indexNumber[0] + 1;
                });
            });
            //获取外键集合
            List<Map<String, String>> foreignKeyList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getForeignKeyList(mysqlSchemaName));
            // 根据约束名称分组
            Map<String, List<Map<String, String>>> groupByF_Map = foreignKeyList.stream().collect(Collectors.groupingBy(doc -> doc.get("CONSTRAINT_NAME")));
            groupByF_Map.forEach((k, v) -> {
                //表名
                String tableName = v.get(0).get("TABLE_NAME");
                //引用的表名
                String referencedTableName = v.get(0).get("REFERENCED_TABLE_NAME");
                //列名
                String columnName = "";
                //引用的列名
                String referencedColumnName = "";
                for (Map<String, String> map : v){
                    String columnNameV = map.get("COLUMN_NAME"); 
                    //校验列名
                    if(createMetaInfo.getTableAndColSwitch() == true){
                        if(SubMetaUtil.colNameIsNormal(columnNameV)){
                            columnNameV = "\\\"" + columnNameV + "\\\"";
                        }
                    }
                    //MySQL增加反单引号
                    if(createMetaInfo.getMysqlCommandSwitch() == true) {
                        columnNameV = "\\`" + columnNameV + "\\`";
                    }
                    columnName = columnName + columnNameV + ",";
                    String referencedColumnNameV = map.get("REFERENCED_COLUMN_NAME"); 
                    //校验列名
                    if(createMetaInfo.getTableAndColSwitch() == true){
                        if(SubMetaUtil.colNameIsNormal(referencedColumnNameV)){
                            referencedColumnNameV = "\\\"" + referencedColumnNameV + "\\\"";
                        }
                    }
                    //MySQL增加反单引号
                    if(createMetaInfo.getMysqlCommandSwitch() == true) {
                        referencedColumnNameV = "\\`" + referencedColumnNameV + "\\`";
                    }
                    referencedColumnName = referencedColumnName + referencedColumnNameV + ",";
                }
                //指定不同的删除和更新行为
                String deleteRule = v.get(0).get("DELETE_RULE");
                String updateRule = v.get(0).get("UPDATE_RULE");
                //约束名称
                String constraintName = v.get(0).get("CONSTRAINT_NAME");
                //最终sql语句
                String sqlText = "";
                if(createMetaInfo.getMysqlCommandSwitch() == false){
                    sqlText = "ALTER TABLE "+haloSchemaName+"."+tableName+" ADD CONSTRAINT "+constraintName+" FOREIGN KEY ("+columnName.replaceAll(",+$", "")+") REFERENCES "+referencedTableName+"("+referencedColumnName.replaceAll(",+$", "")+") ON DELETE "+deleteRule+" ON UPDATE "+updateRule+";";
                } else{
                    sqlText = "ALTER TABLE \\`"+tableName+"\\` ADD CONSTRAINT \\`"+constraintName+"\\` FOREIGN KEY ("+columnName.replaceAll(",+$", "")+") REFERENCES "+referencedTableName+"("+referencedColumnName.replaceAll(",+$", "")+") ON DELETE "+deleteRule+" ON UPDATE "+updateRule+";";
                }
                //任务名称
                String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + tableName + "." + constraintName;
                //添加任务
                subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
            });
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成索引失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成索引脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 生成函数shell文本内容
     * @param createMetaInfo
     * @param jobInfo
     * @param shellMap
     * @param beforeSql
     */
    public ReturnT<String> generateFunctionSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, Map<String, String> shellMap, String beforeSql) {
        try {
            //MySQL模式名称
            String mysqlSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            String filePath = "";
            //Snowflake idworker = new Snowflake(0, 0);
            //获取函数集合
            List<Map<String, String>> functionList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getFunctionList(mysqlSchemaName));
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成中...");
            for (Map<String, String> functionInfo:functionList) {
                String objectName = functionInfo.get("ROUTINE_NAME");
                List<Map<String, String>> functionInfoSql = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getFunctionInfoByName(mysqlSchemaName, objectName));
                String sqlText = functionInfoSql.get(0).get("Create Function");
                // false 继续按原psql执行，true 按mysql执行
                if(createMetaInfo.getMysqlCommandSwitch() == false) {
                    //提取语句块
                    String extractedString = extractSubstring(sqlText, "BEGIN", "END");
                    extractedString = "BEGIN" +extractedString+ "END";
                    //过滤
                    sqlText = sqlText.replaceAll("`", "")
                            .replaceAll("(CREATE)(.*?)(FUNCTION)", "CREATE OR REPLACE FUNCTION")
                            .replaceAll("CHARSET utf8mb4", "")
                            .replaceAll("CHARSET utf8mb3", "")
                            .replace(extractedString, "AS \\$\\$\n"+extractedString+"\n\\$\\$ LANGUAGE plpgsql;");
                } else{
                    //过滤
                    sqlText = sqlText
                            .replaceAll("(CREATE)(.*?)(FUNCTION)", "CREATE OR REPLACE FUNCTION")
                            .replaceAll("CHARSET utf8mb4", "")
                            .replaceAll("CHARSET utf8mb3", "");
                    sqlText = "DELIMITER $$\n\n" + sqlText + "$$\n\nDELIMITER ;";
                    //生成shell脚本返回路径
                    //idworker.nextId() 本来采用唯一id,怕出现错误找文件比较难,暂用函数名称
                    filePath = createFile(sqlText, haloSchemaName+"-"+objectName+".sql", "function");
                    if(filePath.equals("error")){
                        throw new Exception("生成sql脚本文件失败");
                    }
                    jobInfo.setFilePath(filePath);
                    sqlText = "source " + filePath;
                }
                //任务名称
                String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + objectName;
                //添加任务
                subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
            }
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成函数失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成函数脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 生成存储过程shell文本内容
     * @param createMetaInfo
     * @param jobInfo
     * @param shellMap
     * @param beforeSql
     */
    public ReturnT<String> generateProcedureSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, Map<String, String> shellMap, String beforeSql) {
        try {
            //MySQL模式名称
            String mysqlSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            String filePath = "";
            //获取存储过程集合
            List<Map<String, String>> functionList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getProcedureList(mysqlSchemaName));
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成中...");
            for (Map<String, String> functionInfo:functionList) {
                String objectName = functionInfo.get("ROUTINE_NAME");
                List<Map<String, String>> functionInfoSql = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getProcedureInfoByName(mysqlSchemaName, objectName));
                String sqlText = functionInfoSql.get(0).get("Create Procedure");
                // false 继续按原psql执行，true 按mysql执行
                if(createMetaInfo.getMysqlCommandSwitch() == false) {
                    //提取语句块
                    String extractedString = extractSubstring(sqlText, "BEGIN", "END");
                    extractedString = "BEGIN" +extractedString+ "END";
                    //过滤
                    sqlText = sqlText.replaceAll("`", "")
                            .replaceAll("(CREATE)(.*?)(PROCEDURE)", "CREATE OR REPLACE PROCEDURE")
                            .replaceAll("CHARSET utf8mb4", "")
                            .replaceAll("CHARSET utf8mb3", "")
                            .replace(extractedString, "AS \\$\\$\n"+extractedString+"\n\\$\\$ LANGUAGE plpgsql;");
                } else{
                    //过滤
                    sqlText = sqlText
                            .replaceAll("(CREATE)(.*?)(PROCEDURE)", "CREATE OR REPLACE PROCEDURE")
                            .replaceAll("CHARSET utf8mb4", "")
                            .replaceAll("CHARSET utf8mb3", "");
                    sqlText = "DELIMITER $$\n\n" + sqlText + "$$\n\nDELIMITER ;";
                    //生成shell脚本返回路径
                    filePath = createFile(sqlText, haloSchemaName+"-"+objectName+".sql", "procedure");
                    if(filePath.equals("error")){
                        throw new Exception("生成sql脚本文件失败");
                    }
                    jobInfo.setFilePath(filePath);
                    sqlText = "source " + filePath;
                }
                //任务名称
                String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + objectName;
                //添加任务
                subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
            }
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成存储过程失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成存储过程脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 生成视图shell文本内容
     * @param createMetaInfo
     * @param jobInfo
     * @param shellMap
     * @param beforeSql
     */
    public ReturnT<String> generateViewSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, Map<String, String> shellMap, String beforeSql) {
        try {
            //MySQL模式名称
            String mysqlSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            //获取视图集合
            List<Map<String, String>> viewList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getViewList(mysqlSchemaName));
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成中...");
            for (Map<String, String> viewInfo:viewList) {
                //视图名称
                String objectName = viewInfo.get("TABLE_NAME");
                //定义语句
                String VIEW_DEFINITION = viewInfo.get("VIEW_DEFINITION");
                //生成语句
                String sqlText = "";
                if (sqlText.length() < 1) {
                    List<Map<String, String>> functionInfoSql = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getViewListByShow(mysqlSchemaName, objectName));
                    sqlText = functionInfoSql.get(0).get("Create View");
                    //过滤
                    sqlText = sqlText.replaceAll("(CREATE)(.*?)(VIEW)", "CREATE OR REPLACE VIEW");
                } else {
                    sqlText = "CREATE VIEW "+objectName+" AS \n"+VIEW_DEFINITION;
                }
                if(createMetaInfo.getMysqlCommandSwitch() == false) {
                    sqlText = sqlText.replaceAll("`", "");
                }
                // true 按mysql执行
                if(createMetaInfo.getMysqlCommandSwitch() == true) {
                    //生成shell脚本返回路径
                    String filePath = createFile(sqlText, haloSchemaName+"-"+objectName+".sql", "view");
                    if(filePath.equals("error")){
                        throw new Exception("生成sql脚本文件失败");
                    }
                    jobInfo.setFilePath(filePath);
                    sqlText = "source " + filePath;
                }
                //任务名称
                String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + objectName;
                //添加任务
                subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
            }
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成视图失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成视图脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 生成序列shell文本内容
     * @param createMetaInfo
     * @param jobInfo
     * @param shellMap
     * @param beforeSql
     */
    public ReturnT<String> generateSequenceSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, Map<String, String> shellMap, String beforeSql) {
        try {
            //MySQL模式名称
            String mysqlSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            //获取自增列集合
            List<Map<String, String>> dataList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getAutoIncrementList(mysqlSchemaName));
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成中...");
            for (Map<String, String> dataInfo:dataList) {
                //表名
                String TABLE_NAME = dataInfo.get("TABLE_NAME");
                //自动递增值
                String AUTO_INCREMENT = dataInfo.get("AUTO_INCREMENT");
                //列名
                String COLUMN_NAME = dataInfo.get("COLUMN_NAME");
                //序列名称
                String seqName = ""+TABLE_NAME+"_"+COLUMN_NAME+"_seq";
                //最终语句
                String sqlText = "";
                //false: psql  true: mysql
                if(createMetaInfo.getMysqlCommandSwitch() == false){
                    //序列语句
                    String sequenceSqlText = "CREATE SEQUENCE "+seqName+" MINVALUE 1 MAXVALUE 9223372036854775807 INCREMENT BY 1 START WITH "+AUTO_INCREMENT+" CACHE 20;";
                    //列变更语句
                    String alterSqlText = "ALTER TABLE "+TABLE_NAME+" ALTER COLUMN "+COLUMN_NAME+" SET DEFAULT nextval('"+seqName+"');";
                    sqlText = sequenceSqlText + "\n" + alterSqlText;
                } else{
                    //列类型
                    String COLUMN_TYPE = dataInfo.get("COLUMN_TYPE");
                    //MySQL语法
                    sqlText = "ALTER TABLE \\`"+TABLE_NAME+"\\` MODIFY \\`"+COLUMN_NAME+"\\` "+COLUMN_TYPE+" AUTO_INCREMENT,AUTO_INCREMENT="+AUTO_INCREMENT+";";
                }
                //任务名称
                String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + seqName;
                //添加任务
                subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
            }
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成序列失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成序列脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    /**
     * 生成触发器shell文本内容
     * @param createMetaInfo
     * @param jobInfo
     * @param shellMap
     * @param beforeSql
     */
    public ReturnT<String> generateTriggerSQL(CreateMetaPojo createMetaInfo, MetaTypePojo metaTypePojo, JobInfo jobInfo, Map<String, String> shellMap, String beforeSql) {
        try {
            //MySQL模式名称
            String mysqlSchemaName = createMetaInfo.getSchema();
            //Halo模式名称
            String haloSchemaName = createMetaInfo.getSchema();
            //获取触发器集合
            List<Map<String, String>> dataList = getMetaDataUnification(createMetaInfo.getDatasourceId_1(), SubMySQLDatabaseMeta.getTriggerList(mysqlSchemaName));
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成中...");
            for (Map<String, String> dataInfo:dataList) {
                String TRIGGER_NAME = dataInfo.get("TRIGGER_NAME");
                String TRIGGER_BODY = dataInfo.get("ACTION_STATEMENT");
                String TABLE_NAME = dataInfo.get("EVENT_OBJECT_TABLE");
                String TRIGGERING_EVENT = dataInfo.get("EVENT_MANIPULATION");
                String ACTION_TIMING = dataInfo.get("ACTION_TIMING");
                //触发器函数存在则删除
                String function_drop = "DROP FUNCTION IF EXISTS "+TRIGGER_NAME+"_FUNCTION CASCADE;\n";
                // 建立触发器函数
                String function_head = "CREATE FUNCTION "+TRIGGER_NAME+"_FUNCTION() " +
                        "\nRETURNS trigger AS" +
                        "\n\\$\\$\n";
                // true 按mysql执行
                if(createMetaInfo.getMysqlCommandSwitch() == true) {
                    function_drop = "DELIMITER $$\n\n";
                    function_head = "CREATE OR REPLACE FUNCTION "+TRIGGER_NAME+"_FUNCTION() " +
                        "\nRETURNS trigger" +
                        "\n";
                }
                String function_body = TRIGGER_BODY;
                // 对返回值做处理,NEW 只出现在insert和update时,OLD只出现在update和delete时,update都会有,此处暂只对delete做处理
                String function_return = "RETURN NEW;";
                if(TRIGGERING_EVENT.equalsIgnoreCase("DELETE")){
                    function_return = "RETURN OLD;";
                }
                // 对最后一个end之前加上return
                // 创建一个不区分大小写的Pattern
                Pattern pattern = Pattern.compile("(?i)end", Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(function_body);
                // 找到最后一个匹配项的位置
                int lastIndex = -1;
                while (matcher.find()) {
                    lastIndex = matcher.end() - 3; // 减去"end"的长度
                }
                // 如果找到了最后一个"end"，则替换它
                if (lastIndex != -1) {
                    // 使用StringBuilder来修改字符串，因为String是不可变的
                    StringBuilder sb = new StringBuilder(function_body);
                    sb.replace(lastIndex, lastIndex + 3, function_return + "\nEND");
                    function_body = sb.toString();
                }
                String function_bottom = "\n\\$\\$\nLANGUAGE \"plpgsql\";";
                // true 按mysql执行
                if(createMetaInfo.getMysqlCommandSwitch() == true) {
                    function_bottom = "$$\n\nDELIMITER ;";
                }
                // 最终函数语句
                String functionSqlText = function_drop + function_head + function_body + function_bottom;
                //触发器存在则删除
                String trigger_drop = "DROP TRIGGER IF EXISTS "+TRIGGER_NAME+"\n" +
                        "ON "+TABLE_NAME+" CASCADE;\n";
                // 建立触发器
                String triggerSqlText = "CREATE TRIGGER "+TRIGGER_NAME+" " +
                        "\n"+ACTION_TIMING+" "+TRIGGERING_EVENT+" " +
                        "\nON "+TABLE_NAME+" " +
                        "\nFOR EACH ROW" +
                        "\nEXECUTE FUNCTION "+TRIGGER_NAME+"_FUNCTION();";
                // true 按mysql执行
                if(createMetaInfo.getMysqlCommandSwitch() == true) {
                    trigger_drop = "\nDELIMITER $$\n\n" + trigger_drop;
                    triggerSqlText = triggerSqlText + "$$\n\nDELIMITER ;";
                }
                String sqlText = functionSqlText + "\n" + trigger_drop + triggerSqlText;
                //任务名称
                String jobDesc = metaTypePojo.getMetaTypeName() + ":" + haloSchemaName + "." + TRIGGER_NAME;
                // true 按mysql执行
                if(createMetaInfo.getMysqlCommandSwitch() == true) {
                    //生成shell脚本返回路径
                    String filePath = createFile(sqlText, haloSchemaName+"-"+TRIGGER_NAME+".sql", "trigger");
                    if(filePath.equals("error")){
                        throw new Exception("生成sql脚本文件失败");
                    }
                    jobInfo.setFilePath(filePath);
                    sqlText = "source " + filePath;
                }
                //添加任务
                subMetaDataService.jsonBuild(jobInfo, shellMap, jobDesc, sqlText, createMetaInfo.getMetaId(), SubMetaEnum.BUILD_TYPE_1.getTypeId());
            }
            logger.info("----------------- "+metaTypePojo.getMetaTypeName()+"脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成触发器失败:",e);
            TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
            return new ReturnT<>(101, "生成触发器脚本任务失败:"+e.getMessage());
        }
        return ReturnT.SUCCESS;
    }

    @Override
    public List<Map<String, String>> generateComparePublicSQL(String mysqlSchemaName, Long datasourceId, int metaTypeId) {
        List<Map<String, String>> constraintList = new ArrayList<>();
        if(metaTypeId == SubMetaTypeEnum.FUNCTION.getMetaTypeId()){
            constraintList = generateCompareFunctionSQL(mysqlSchemaName, datasourceId, metaTypeId);
        } else if(metaTypeId == SubMetaTypeEnum.PROCEDURE.getMetaTypeId()){
            constraintList = generateCompareProcedureSQL(mysqlSchemaName, datasourceId, metaTypeId);
        } else if(metaTypeId == SubMetaTypeEnum.VIEW.getMetaTypeId()){
            constraintList = generateCompareViewSQL(mysqlSchemaName, datasourceId, metaTypeId);
        } else if(metaTypeId == SubMetaTypeEnum.TRIGGER.getMetaTypeId()){
            constraintList = generateCompareTriggerSQL(mysqlSchemaName, datasourceId, metaTypeId);
        }
        return constraintList;
    }

    /**
     * 生成函数语句集合(用于对比)
     * @param mysqlSchemaName
     * @param datasourceId
     * @param metaTypeId
     * @return
     */
    public List<Map<String, String>> generateCompareFunctionSQL(String mysqlSchemaName, Long datasourceId, int metaTypeId) {
        List<Map<String, String>> constraintList = new ArrayList<>();
        try {
            MetaTypePojo metaTypePojo = new MetaTypePojo();
            metaTypePojo.setObjectType(SubMetaTypeEnum.FUNCTION.getObjectType());
            metaTypePojo.setMetaTypeId(SubMetaTypeEnum.FUNCTION.getMetaTypeId());
            //获取函数集合
            List<Map<String, String>> functionList = getMetaDataUnification(datasourceId, SubMySQLDatabaseMeta.getFunctionList(mysqlSchemaName));
            logger.info("----------------- 正在生成函数对比脚本任务...");
            for (Map<String, String> functionInfo:functionList) {
                String objectName = functionInfo.get("ROUTINE_NAME");
                List<Map<String, String>> functionInfoSql = getMetaDataUnification(datasourceId, SubMySQLDatabaseMeta.getFunctionInfoByName(mysqlSchemaName, objectName));
                String sqlText = functionInfoSql.get(0).get("Create Function");
                //过滤
                sqlText = sqlText
                .replaceAll("`", "")
                .replaceAll("(CREATE)(.*?)(FUNCTION)", "CREATE FUNCTION")
                .replaceAll("CHARSET utf8mb4", "")
                .replaceAll("CHARSET utf8mb3", "")
                .replaceAll(" RETURNS ", "\n RETURNS ");
                //sqlText = "DELIMITER $$\n\n" + sqlText + "$$\n\nDELIMITER ;";
                Map<String, String> objMap = new HashMap<>();
                objMap.put("OBJECT_NAME", objectName);
                objMap.put("SQL", sqlText);
                constraintList.add(objMap);
            }
            logger.info("----------------- 函数对比脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成对比函数失败:",e);
        }
        return constraintList;
    }

    /**
     * 生成存过语句集合(用于对比)
     * @param mysqlSchemaName
     * @param datasourceId
     * @param metaTypeId
     * @return
     */
    public List<Map<String, String>> generateCompareProcedureSQL(String mysqlSchemaName, Long datasourceId, int metaTypeId) {
        List<Map<String, String>> constraintList = new ArrayList<>();
        try {
            MetaTypePojo metaTypePojo = new MetaTypePojo();
            metaTypePojo.setObjectType(SubMetaTypeEnum.PROCEDURE.getObjectType());
            metaTypePojo.setMetaTypeId(SubMetaTypeEnum.PROCEDURE.getMetaTypeId());
            //获取存储过程集合
            List<Map<String, String>> functionList = getMetaDataUnification(datasourceId, SubMySQLDatabaseMeta.getProcedureList(mysqlSchemaName));
            logger.info("----------------- 正在生成存储过程对比脚本任务...");
            for (Map<String, String> functionInfo:functionList) {
                String objectName = functionInfo.get("ROUTINE_NAME");
                List<Map<String, String>> functionInfoSql = getMetaDataUnification(datasourceId, SubMySQLDatabaseMeta.getProcedureInfoByName(mysqlSchemaName, objectName));
                String sqlText = functionInfoSql.get(0).get("Create Procedure");
                //提取语句块
                String extractedString = extractSubstring(sqlText, "BEGIN", "END");
                extractedString = "BEGIN" +extractedString+ "END";
                //过滤
                sqlText = sqlText.replaceAll("`", "")
                        .replaceAll("(CREATE)(.*?)(PROCEDURE)", "CREATE PROCEDURE")
                        .replaceAll("CHARSET utf8mb4", "")
                        .replaceAll("CHARSET utf8mb3", "");
                Map<String, String> objMap = new HashMap<>();
                objMap.put("OBJECT_NAME", objectName);
                objMap.put("SQL", sqlText);
                constraintList.add(objMap);
            }
            logger.info("----------------- 存储过程对比脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成存储过程对比脚本任务失败:",e);
        }
        return constraintList;
    }

    /**
     * 生成视图语句集合(用于对比)
     * @param mysqlSchemaName
     * @param datasourceId
     * @param metaTypeId
     * @return
     */
    public List<Map<String, String>> generateCompareViewSQL(String mysqlSchemaName, Long datasourceId, int metaTypeId) {
        List<Map<String, String>> constraintList = new ArrayList<>();
        try {
            MetaTypePojo metaTypePojo = new MetaTypePojo();
            metaTypePojo.setObjectType(SubMetaTypeEnum.VIEW.getObjectType());
            metaTypePojo.setMetaTypeId(SubMetaTypeEnum.VIEW.getMetaTypeId());
            //获取视图集合
            List<Map<String, String>> viewList = getMetaDataUnification(datasourceId, SubMySQLDatabaseMeta.getViewList(mysqlSchemaName));
            logger.info("----------------- 正在生成视图对比脚本任务...");
            for (Map<String, String> viewInfo:viewList) {
                //视图名称
                String objectName = viewInfo.get("TABLE_NAME");
                //定义语句
                String VIEW_DEFINITION = viewInfo.get("VIEW_DEFINITION");
                //生成语句
                String sqlText = "";
                if (sqlText.length() < 1) {
                    List<Map<String, String>> functionInfoSql = getMetaDataUnification(datasourceId, SubMySQLDatabaseMeta.getViewListByShow(mysqlSchemaName, objectName));
                    sqlText = functionInfoSql.get(0).get("Create View");
                    //过滤
                    sqlText = sqlText
                            .replaceAll("(CREATE)(.*?)(VIEW)", "CREATE VIEW")
                            .replaceAll("`", "");
                } else {
                    sqlText = VIEW_DEFINITION.replaceAll("`", "");
                    sqlText = "CREATE VIEW "+objectName+" AS \n"+sqlText;
                }
                Map<String, String> objMap = new HashMap<>();
                objMap.put("OBJECT_NAME", objectName);
                objMap.put("SQL", sqlText);
                constraintList.add(objMap);
            }
            logger.info("----------------- 视图对比脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成视图对比失败:",e);
        }
        return constraintList;
    }

    /**
     * 生成触发器语句集合(用于对比)
     * @param mysqlSchemaName
     * @param datasourceId
     * @param metaTypeId
     * @return
     */
    public List<Map<String, String>> generateCompareTriggerSQL(String mysqlSchemaName, Long datasourceId, int metaTypeId) {
        List<Map<String, String>> constraintList = new ArrayList<>();
        try {
            MetaTypePojo metaTypePojo = new MetaTypePojo();
            metaTypePojo.setObjectType(SubMetaTypeEnum.VIEW.getObjectType());
            metaTypePojo.setMetaTypeId(SubMetaTypeEnum.VIEW.getMetaTypeId());
            //获取触发器集合
            List<Map<String, String>> dataList = getMetaDataUnification(datasourceId, SubMySQLDatabaseMeta.getTriggerList(mysqlSchemaName));
            logger.info("----------------- 正在生成触发器对比脚本任务...");
            for (Map<String, String> dataInfo:dataList) {
                String TRIGGER_NAME = dataInfo.get("TRIGGER_NAME");
                String TRIGGER_BODY = dataInfo.get("ACTION_STATEMENT");
                String TABLE_NAME = dataInfo.get("EVENT_OBJECT_TABLE");
                String TRIGGERING_EVENT = dataInfo.get("EVENT_MANIPULATION");
                String ACTION_TIMING = dataInfo.get("ACTION_TIMING");
                //触发器函数存在则删除
                String function_drop = "";
                // 建立触发器函数
                String function_head = "CREATE OR REPLACE FUNCTION "+TRIGGER_NAME+"_FUNCTION() " +
                        "\nRETURNS trigger" +
                        "\n";
                String function_body = TRIGGER_BODY;
                // 对返回值做处理,NEW 只出现在insert和update时,OLD只出现在update和delete时,update都会有,此处暂只对delete做处理
                String function_return = "RETURN NEW;";
                if(TRIGGERING_EVENT.equalsIgnoreCase("DELETE")){
                    function_return = "RETURN OLD;";
                }
                // 对最后一个end之前加上return
                // 创建一个不区分大小写的Pattern
                Pattern pattern = Pattern.compile("(?i)end", Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(function_body);
                // 找到最后一个匹配项的位置
                int lastIndex = -1;
                while (matcher.find()) {
                    lastIndex = matcher.end() - 3; // 减去"end"的长度
                }
                // 如果找到了最后一个"end"，则替换它
                if (lastIndex != -1) {
                    // 使用StringBuilder来修改字符串，因为String是不可变的
                    StringBuilder sb = new StringBuilder(function_body);
                    sb.replace(lastIndex, lastIndex + 3, function_return + "\nEND");
                    function_body = sb.toString();
                }
                String function_bottom = "";
                // 最终函数语句
                String functionSqlText = function_drop + function_head + function_body + function_bottom;
                //触发器存在则删除
                String trigger_drop = "\n";
                // 建立触发器
                String triggerSqlText = "CREATE TRIGGER "+TRIGGER_NAME+" " +
                        "\n"+ACTION_TIMING+" "+TRIGGERING_EVENT+" " +
                        "\nON "+TABLE_NAME+" " +
                        "\nFOR EACH ROW" +
                        "\nEXECUTE FUNCTION "+TRIGGER_NAME+"_FUNCTION();";

                trigger_drop = "" + trigger_drop;
                triggerSqlText = triggerSqlText + "";
                String sqlText = functionSqlText + "\n" + trigger_drop + triggerSqlText;
                Map<String, String> objMap = new HashMap<>();
                objMap.put("OBJECT_NAME", TRIGGER_NAME);
                objMap.put("SQL", sqlText);
                constraintList.add(objMap);
            }
            logger.info("----------------- 触发器对比脚本任务生成完毕");
        } catch (Exception e) {
            logger.error("生成触发器失败:",e);
        }
        return constraintList;
    }


    /**
     * 根据分区类型变化
     * @return
     */
    public static Map<String, String> disposePartPartitioningTypeSQL(String PARTITIONING_TYPE, String HIGH_VALUE, String old_value, int partitionCount, int series, String create_table_tmp){
        Map<String, String> map = new HashMap<>();
        String new_value = "";
        //根据分区类型变化
        if(PARTITIONING_TYPE.equals("LIST")){
            if(HIGH_VALUE.equalsIgnoreCase("DEFAULT")){
                create_table_tmp = create_table_tmp + "DEFAULT;\n";
            } else{
                create_table_tmp = create_table_tmp + "FOR VALUES IN ("+HIGH_VALUE+");\n";
            }
        } else if(PARTITIONING_TYPE.equals("RANGE")){
            new_value = disposePartRange(HIGH_VALUE);
            create_table_tmp = create_table_tmp + "FOR VALUES FROM ("+old_value+") TO ("+new_value+");\n";
        } else if(PARTITIONING_TYPE.equals("HASH") || PARTITIONING_TYPE.equals("KEY")){
            create_table_tmp = create_table_tmp + "FOR VALUES WITH (MODULUS "+partitionCount+",REMAINDER "+(series)+");\n";
        }
        map.put("new_value", new_value);
        map.put("create_table_tmp", create_table_tmp);
        return map;
    }

    /**
     * 处理分区表的Range
     * @param HIGH_VALUE
     * @return
     */
    public static String disposePartRange(String HIGH_VALUE){
        String new_value = HIGH_VALUE;
        if(new_value.contains("TO_DATE")) {
            String regexc = "(?<=\')([^\\.])(.*?)(?=\')";
            Pattern pattern = Pattern.compile(regexc);
            Matcher matcher = pattern.matcher(new_value);
            if (matcher.find()) {
                new_value = "\'" + matcher.group(0) + '\'';
            }
        }
        return new_value;
    }

    public List<Map<String, String>> getMetaDataUnification(Long id, String executeSql) {
        //获取数据源对象
        JobDatasource datasource = jobDatasourceService.getById(id);
        //queryTool组装
        if (ObjectUtil.isNull(datasource)) {
            return Lists.newArrayList();
        }
        BaseQueryTool qTool = QueryToolFactory.getByDbType(datasource);
        return qTool.getMetaDataUnificationSql(executeSql);
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

    private static String extractSubstring(String input, String startMarker, String endMarker) {
        String regex = Pattern.quote(startMarker) + "(.*)" + Pattern.quote(endMarker);
        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(input);
        if (matcher.find()) {
            return matcher.group(1);
        }
        return null;
    }


}
