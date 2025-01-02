package com.wugui.hmt.admin.service.impl;

import com.google.common.collect.Lists;
import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.admin.entity.JobDataContrast;
import com.wugui.hmt.admin.entity.JobDataContrastDetails;
import com.wugui.hmt.admin.entity.JobDataTypeMapping;
import com.wugui.hmt.admin.entity.JobDatasource;
import com.wugui.hmt.admin.mapper.JobDataContrastDetailsMapper;
import com.wugui.hmt.admin.mapper.JobDataContrastMapper;
import com.wugui.hmt.admin.mapper.JobDataTypeMappingMapper;
import com.wugui.hmt.admin.service.*;
import com.wugui.hmt.admin.tool.meta.SubHaloDatabaseMeta;
import com.wugui.hmt.admin.tool.meta.SubMySQLDatabaseMeta;
import com.wugui.hmt.admin.tool.query.BaseQueryTool;
import com.wugui.hmt.admin.tool.query.QueryToolFactory;
import com.wugui.hmt.admin.util.SubMetaTypeEnum;
import com.wugui.hmt.admin.util.SubMetaUtil;

import cn.hutool.core.util.ObjectUtil;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.logging.log4j.util.Strings.isBlank;


/*-------------------------------------------------------------------------
 *
 * JobDataContrastServiceImpl.java
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/service/impl/JobDataContrastServiceImpl.java
 *
 *-----------------------------------------------
 */
@Service
public class JobDataContrastServiceImpl implements JobDataContrastService {

    @Resource
    private DatasourceQueryService datasourceQueryService;

    @Resource
    private JobDatasourceService jobDatasourceService;

    @Resource
    private JobDataContrastMapper jobDataContrastMapper;

    @Resource
    private JobDataContrastDetailsMapper jobDataContrastDetailsMapper;

    @Resource
    private JobDataTypeMappingMapper jobDataTypeMappingMapper;

    @Resource
    private SubMetaDataOracleService subMetaDataOracleService;

    @Resource
    private SubMetaDataMySQLService subMetaDataMySQLService;

    @Resource
    private SubMetaDataHaloService subHaloDataService;

    @Override
    @Transactional(rollbackFor = Exception.class)
    public ReturnT<String> createDataContrastTask(JobDataContrast jobDataContrast) {
        try {
            //传输的参数
            Long readerDatasourceId = jobDataContrast.getReaderDatasourceId();
            String readerSchema = jobDataContrast.getReaderSchema();
            Long writerDatasourceId = jobDataContrast.getWriterDatasourceId();
            String writerSchema = jobDataContrast.getWriterSchema();
            jobDataContrast.setMetaType("0");
            //保存任务
            int addResult = jobDataContrastMapper.save(jobDataContrast);
            //任务id
            int taskId = jobDataContrast.getId();
            //数据源
            JobDatasource readerDatasource = jobDatasourceService.getById(readerDatasourceId);
            JobDatasource writerDatasource = jobDatasourceService.getById(writerDatasourceId);
            //源端所有表
            List<String> readerTableNames = datasourceQueryService.getTablesNoSchema(readerDatasource,readerSchema);
            //目标端所有表
            List<String> writerTableNames = datasourceQueryService.getTablesNoSchema(writerDatasource,writerSchema);
            //先找出源端表对应的数据行数
            for (String readerTableName:readerTableNames) {
                String usedReaderTableName = readerTableName;
                // readerTableName = resultOracleTableName(readerSchema, readerTableName);
                if ("mysql".equalsIgnoreCase(readerDatasource.getDatasource())) {
                    readerTableName = resultMysqlTableName(readerSchema, readerTableName);
                } else {
                    readerTableName = resultOracleTableName(readerSchema, readerTableName);
                }
                //获取表行数
                Long readerTableRows = datasourceQueryService.getTableRows(readerDatasource,readerTableName);
                //源端表行数与目标端表行数是否一致
                int isCorrect = 1;
                String writerTableName = "";
                Long writerTableRows = 0L;
                //查询目标端是否存在该表
                List<String> filteredWriterNames = writerTableNames.stream().filter(s -> s.equalsIgnoreCase(usedReaderTableName)).collect(Collectors.toList());
                if(!filteredWriterNames.isEmpty()){
                    writerTableName = filteredWriterNames.get(0);
                    //匹配到则从集合里面删除
                    writerTableNames.remove(writerTableName);
                    writerTableName = resultHaloTableName(writerSchema, writerTableName);
                    //获取表行数
                    writerTableRows = datasourceQueryService.getTableRows(writerDatasource,writerTableName);
                    if(readerTableRows.equals(writerTableRows)){
                        isCorrect = 0;
                    }
                }
                //保存详情
                JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
                jobDataContrastDetails.setTaskId(taskId);
                jobDataContrastDetails.setReaderTable(readerTableName);
                jobDataContrastDetails.setReaderRecordRows(readerTableRows);
                jobDataContrastDetails.setWriterTable(writerTableName);
                jobDataContrastDetails.setWriterRecordRows(writerTableRows);
                jobDataContrastDetails.setIsCorrect(isCorrect);
                jobDataContrastDetailsMapper.save(jobDataContrastDetails);
            }
            //处理目标端剩下的表
            for (String writerTableName:writerTableNames) {
                writerTableName = resultHaloTableName(writerSchema, writerTableName);
                //获取表行数
                Long writerTableRows = datasourceQueryService.getTableRows(writerDatasource,writerTableName);
                //保存详情
                JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
                jobDataContrastDetails.setTaskId(taskId);
                jobDataContrastDetails.setReaderTable("");
                jobDataContrastDetails.setReaderRecordRows(0L);
                jobDataContrastDetails.setWriterTable(writerTableName);
                jobDataContrastDetails.setWriterRecordRows(writerTableRows);
                jobDataContrastDetails.setIsCorrect(1);
                jobDataContrastDetailsMapper.save(jobDataContrastDetails);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ReturnT.SUCCESS;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public ReturnT<String> createStructureContrastTask(JobDataContrast jobDataContrast) {
        //数据源
        JobDatasource readerDatasource = jobDatasourceService.getById(jobDataContrast.getReaderDatasourceId());
        String datasource = readerDatasource.getDatasource();
        if (datasource.equals("oracle")){
            return createStructureContrastTaskOracle(jobDataContrast);
        } else if(datasource.equals("mysql")){
            return createStructureContrastTaskMySQL(jobDataContrast);
        }
        return ReturnT.SUCCESS;
    }

    /**
     * Oracle对比
     * @param jobDataContrast
     * @return
     */
    public ReturnT<String> createStructureContrastTaskOracle(JobDataContrast jobDataContrast) {
        try {
            //传输的参数
            Long readerDatasourceId = jobDataContrast.getReaderDatasourceId();
            String readerSchema = jobDataContrast.getReaderSchema();
            Long writerDatasourceId = jobDataContrast.getWriterDatasourceId();
            String writerSchema = jobDataContrast.getWriterSchema();
            jobDataContrast.setMetaType("2");
            //保存任务
            int addResult = jobDataContrastMapper.save(jobDataContrast);
            //任务id
            int taskId = jobDataContrast.getId();
            //数据源
            JobDatasource readerDatasource = jobDatasourceService.getById(readerDatasourceId);
            JobDatasource writerDatasource = jobDatasourceService.getById(writerDatasourceId);
            //查询数据类型映射集合,用于后续类型对比
            List<JobDataTypeMapping> jobDataTypeMappingList = jobDataTypeMappingMapper.getListBydatasource(readerDatasource.getDatasource());
            //源端所有表
            List<String> readerTableNames = datasourceQueryService.getTablesNoSchema(readerDatasource,readerSchema);
            //目标端所有表
            List<String> writerTableNames = datasourceQueryService.getTablesNoSchema(writerDatasource,writerSchema);
            //先找出源端表对应的字段信息
            for (String readerTableName:readerTableNames) {
                String usedReaderTableName = readerTableName;
                //源端表字段信息
                List<Map<String, String>> readerColumnsList = datasourceQueryService.getTableColumnsAndType(readerDatasource, readerSchema, readerTableName);
                //目标表字段信息
                List<Map<String, String>> writerColumnsList = new ArrayList<>();
                //源端表字段信息与目标端表字段信息是否一致
                int isCorrect = 1;
                String writerTableName = "";
                //查询目标端是否存在该表
                List<String> filteredWriterNames = writerTableNames.stream().filter(s -> s.equalsIgnoreCase(usedReaderTableName)).collect(Collectors.toList());
                if(!filteredWriterNames.isEmpty()){
                    writerTableName = filteredWriterNames.get(0);
                    //匹配到则从集合里面删除
                    writerTableNames.remove(writerTableName);
                    //目标表字段信息
                    writerColumnsList = datasourceQueryService.getTableColumnsAndType(writerDatasource, writerSchema, writerTableName);
                }
                //先将语法语句保存
                String writerInfo = SubMetaUtil.createTableSQL(writerTableName, writerColumnsList);
                String readerInfo = SubMetaUtil.createTableSQL(readerTableName, readerColumnsList);
                //将语法转换成Oracle映射前的，并和Oracle方进行对比
                List<Map<String, String>> writerColumnsList2 = writerColumnsList;
                for (Map<String, String> map:writerColumnsList2) {
                    String colName = map.get("COLUMN_NAME");
                    String colDataType = map.get("DATA_TYPE");
                    if(!SubMetaUtil.oraColNameIsNormal(colName)){
                        map.put("COLUMN_NAME", colName.toUpperCase());
                    }
                    //查找源端的这个字段
                    List<Map<String, String>> re = readerColumnsList.stream()
                            .filter(m -> m.get("COLUMN_NAME").equalsIgnoreCase(colName))
                            .collect(Collectors.toList());
                    if(re.isEmpty()){
                        map.put("DATA_TYPE", colDataType);
                    } else{
                        map.put("DATA_TYPE", SubMetaUtil.getOracleAndHaloDataTypeMapping(re.get(0).get("DATA_TYPE"), colDataType, jobDataTypeMappingList));
                    }
                }
                String mappingInfo = SubMetaUtil.createTableSQL(writerTableName, writerColumnsList2);
                //对比前,将类型长度去除,不参与对比
                List<Map<String, String>> updateReaderColumnsList = convertingOracleColumnsList("reader", readerColumnsList);
                List<Map<String, String>> updateWriterColumnsList = convertingOracleColumnsList("writer", writerColumnsList2);
                //对比
                if(updateReaderColumnsList.equals(updateWriterColumnsList)){
                    isCorrect = 0;
                }
                //保存详情
                JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
                jobDataContrastDetails.setTaskId(taskId);
                jobDataContrastDetails.setReaderTable(resultOracleTableName(readerSchema, readerTableName));
                jobDataContrastDetails.setReaderRecordRows(0L);
                jobDataContrastDetails.setWriterTable(resultHaloTableName(writerSchema, writerTableName));
                jobDataContrastDetails.setWriterRecordRows(0L);
                jobDataContrastDetails.setIsCorrect(isCorrect);
                jobDataContrastDetails.setReaderInfo(readerInfo);
                jobDataContrastDetails.setWriterInfo(writerInfo);
                //判断表名是否需要转换大写
                if(!SubMetaUtil.oraColNameIsNormal(writerTableName)){
                    writerTableName = writerTableName.toUpperCase();
                }
                jobDataContrastDetails.setMappingInfo(mappingInfo);
                jobDataContrastDetails.setMetaTypeId("2");
                jobDataContrastDetailsMapper.save(jobDataContrastDetails);
            }
            //处理目标端剩下的表
            for (String writerTableName:writerTableNames) {
                List<Map<String, String>> writerColumnsList = datasourceQueryService.getTableColumnsAndType(writerDatasource, writerSchema, writerTableName);
                //保存详情
                JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
                jobDataContrastDetails.setTaskId(taskId);
                jobDataContrastDetails.setReaderTable("");
                jobDataContrastDetails.setReaderRecordRows(0L);
                jobDataContrastDetails.setWriterTable(resultHaloTableName(writerSchema, writerTableName));
                jobDataContrastDetails.setWriterRecordRows(0L);
                jobDataContrastDetails.setIsCorrect(1);
                jobDataContrastDetails.setWriterInfo(SubMetaUtil.createTableSQL(writerTableName, writerColumnsList));
                jobDataContrastDetails.setMetaTypeId("2");
                jobDataContrastDetailsMapper.save(jobDataContrastDetails);
            }
            //自定义类型对比
            contrastPublic(taskId, jobDataContrast, SubMetaTypeEnum.TYPE.getMetaTypeId());
            //约束对比
            contrastConstraint(taskId, jobDataContrast);
            //索引对比
            contrastIndex(taskId, jobDataContrast);
            //函数对比
            contrastPublic(taskId, jobDataContrast, SubMetaTypeEnum.FUNCTION.getMetaTypeId());
            //存储过程对比
            contrastPublic(taskId, jobDataContrast, SubMetaTypeEnum.PROCEDURE.getMetaTypeId());
            //视图对比
            contrastPublic(taskId, jobDataContrast, SubMetaTypeEnum.VIEW.getMetaTypeId());
            //序列对比
            contrastPublic(taskId, jobDataContrast, SubMetaTypeEnum.SEQUENCE.getMetaTypeId());
            //触发器对比
            contrastPublic(taskId, jobDataContrast, SubMetaTypeEnum.TRIGGER.getMetaTypeId());
            //包对比
            contrastPublic(taskId, jobDataContrast, SubMetaTypeEnum.PACKAGE.getMetaTypeId());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ReturnT.SUCCESS;
    }

    /**
     * MySQL对比
     * @param jobDataContrast
     * @return
     */
    public ReturnT<String> createStructureContrastTaskMySQL(JobDataContrast jobDataContrast) {
        try {
            //传输的参数
            Long readerDatasourceId = jobDataContrast.getReaderDatasourceId();
            String readerSchema = jobDataContrast.getReaderSchema();
            Long writerDatasourceId = jobDataContrast.getWriterDatasourceId();
            String writerSchema = jobDataContrast.getWriterSchema();
            jobDataContrast.setMetaType("2");
            //保存任务
            int addResult = jobDataContrastMapper.save(jobDataContrast);
            //任务id
            int taskId = jobDataContrast.getId();
            //数据源
            JobDatasource readerDatasource = jobDatasourceService.getById(readerDatasourceId);
            JobDatasource writerDatasource = jobDatasourceService.getById(writerDatasourceId);
            //查询数据类型映射集合,用于后续类型对比
            List<JobDataTypeMapping> jobDataTypeMappingList = jobDataTypeMappingMapper.getListBydatasource(readerDatasource.getDatasource());
            //源端所有表
            List<String> readerTableNames = datasourceQueryService.getTablesNoSchema(readerDatasource,readerSchema);
            //目标端所有表
            List<String> writerTableNames = datasourceQueryService.getTablesNoSchema(writerDatasource,writerSchema);
            //先找出源端表对应的字段信息
            for (String readerTableName:readerTableNames) {
                String usedReaderTableName = readerTableName;
                //源端表字段信息
                List<Map<String, String>> readerColumnsList = datasourceQueryService.getTableColumnsAndType(readerDatasource, readerSchema, readerTableName);
                //目标表字段信息
                List<Map<String, String>> writerColumnsList = new ArrayList<>();
                //源端表字段信息与目标端表字段信息是否一致
                int isCorrect = 1;
                String writerTableName = "";
                //查询目标端是否存在该表
                List<String> filteredWriterNames = writerTableNames.stream().filter(s -> s.equalsIgnoreCase(usedReaderTableName)).collect(Collectors.toList());
                if(!filteredWriterNames.isEmpty()){
                    writerTableName = filteredWriterNames.get(0);
                    //匹配到则从集合里面删除
                    writerTableNames.remove(writerTableName);
                    //目标表字段信息
                    writerColumnsList = datasourceQueryService.getTableColumnsAndType(writerDatasource, writerSchema, writerTableName);
                }
                //获取索引集合
                List<Map<String, String>> readerIndexList = getMetaDataUnification(readerDatasourceId, SubMySQLDatabaseMeta.getIndexListByTableName(readerSchema, readerTableName));
                List<Map<String, String>> writerIndexList = getMetaDataUnification(writerDatasourceId, SubHaloDatabaseMeta.getIndexTypeListByTableName(writerSchema, writerTableName));
                //获取外键集合
                List<Map<String, String>> readerForeignKeyList = getMetaDataUnification(readerDatasourceId, SubMySQLDatabaseMeta.getForeignKeyListByTableName(readerSchema, readerTableName));
                List<Map<String, String>> writerForeignKeyList = getMetaDataUnification(writerDatasourceId, SubHaloDatabaseMeta.getForeignKeyListByTableName(writerSchema, writerTableName));
                //先将语法语句保存
                Map<String, String> writerInfoMap = SubMetaUtil.createMySQLTableSQL("writer", writerTableName, writerColumnsList, writerIndexList, writerForeignKeyList);
                Map<String, String> readerInfoMap = SubMetaUtil.createMySQLTableSQL("reader", readerTableName, readerColumnsList, readerIndexList, readerForeignKeyList);
                //将语法转换成MySQL映射前的，并和MySQL方进行对比
                List<Map<String, String>> writerColumnsList2 = writerColumnsList;
                for (Map<String, String> map:writerColumnsList2) {
                    String colName = map.get("COLUMN_NAME");
                    String colDataType = map.get("DATA_TYPE");
                    map.put("COLUMN_NAME", colName);
                    //查找源端的这个字段
                    List<Map<String, String>> re = readerColumnsList.stream()
                            .filter(m -> m.get("COLUMN_NAME").equalsIgnoreCase(colName))
                            .collect(Collectors.toList());
                    if(re.isEmpty()){
                        map.put("DATA_TYPE", colDataType);
                    } else{
                        map.put("DATA_TYPE", SubMetaUtil.getMySQLAndHaloDataTypeMapping(re.get(0).get("DATA_TYPE"), colDataType, jobDataTypeMappingList));
                    }
                }
                //目标端映射前
                Map<String, String> mappingInfoMap = SubMetaUtil.createMySQLTableSQL("mapping", writerTableName, writerColumnsList2, writerIndexList, writerForeignKeyList);
                //对比前,将类型长度去除,不参与对比
                List<Map<String, String>> updateReaderColumnsList = convertingMySQLColumnsList("reader", readerColumnsList);
                List<Map<String, String>> updateWriterColumnsList = convertingMySQLColumnsList("writer", writerColumnsList2);
                //简易表结构对比对比
                if(updateReaderColumnsList.equals(updateWriterColumnsList)){
                    isCorrect = 0;
                }
                //其他对比(索引、外键),因为保证不了顺序和MySQL一致,所以采用List<String>的方式进行比较
                List<String> readerList = Arrays.asList(readerInfoMap.get("otherSqlText").split(","));
                List<String> mappingList = Arrays.asList(mappingInfoMap.get("otherSqlText").split(","));
                //不匹配则设置1
                if(!areListsEqualIgnoreCase(readerList, mappingList)){
                    isCorrect = 1;
                }
                // // 用于对比测试
                // if(readerTableName.equals("dc_dictionary")){
                //     // 遍历List，并打印每个Map的内容  
                //     for (Map<String, String> map : updateReaderColumnsList) {  
                //         // 遍历Map，并打印每个键值对  
                //         for (Map.Entry<String, String> entry : map.entrySet()) {  
                //             System.out.println("1:"+entry.getKey() + ": " + entry.getValue());  
                //         }  
                //         // 如果你希望在每个Map的内容后打印一个分隔符（比如空行），可以在这里添加  
                //         System.out.println();  
                //     }  
                //     // 遍历List，并打印每个Map的内容  
                //     for (Map<String, String> map : updateWriterColumnsList) {  
                //         // 遍历Map，并打印每个键值对  
                //         for (Map.Entry<String, String> entry : map.entrySet()) {  
                //             System.out.println("2:"+entry.getKey() + ": " + entry.getValue());  
                //         }  
                //         // 如果你希望在每个Map的内容后打印一个分隔符（比如空行），可以在这里添加  
                //         System.out.println();  
                //     }  
                // }
                //保存详情
                JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
                jobDataContrastDetails.setTaskId(taskId);
                jobDataContrastDetails.setReaderTable(resultMysqlTableName(readerSchema, readerTableName));
                jobDataContrastDetails.setReaderRecordRows(0L);
                jobDataContrastDetails.setWriterTable(resultHaloTableName(writerSchema, writerTableName));
                jobDataContrastDetails.setWriterRecordRows(0L);
                jobDataContrastDetails.setIsCorrect(isCorrect);
                jobDataContrastDetails.setReaderInfo(readerInfoMap.get("sqlText"));
                jobDataContrastDetails.setWriterInfo(writerInfoMap.get("sqlText"));
                jobDataContrastDetails.setMappingInfo(mappingInfoMap.get("sqlText"));
                jobDataContrastDetails.setMetaTypeId("2");
                jobDataContrastDetailsMapper.save(jobDataContrastDetails);
            }
            //处理目标端剩下的表
            for (String writerTableName:writerTableNames) {
                List<Map<String, String>> writerColumnsList = datasourceQueryService.getTableColumnsAndType(writerDatasource, writerSchema, writerTableName);
                //保存详情
                JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
                jobDataContrastDetails.setTaskId(taskId);
                jobDataContrastDetails.setReaderTable("");
                jobDataContrastDetails.setReaderRecordRows(0L);
                jobDataContrastDetails.setWriterTable(resultHaloTableName(writerSchema, writerTableName));
                jobDataContrastDetails.setWriterRecordRows(0L);
                jobDataContrastDetails.setIsCorrect(1);
                Map<String, String> writerInfoMap = SubMetaUtil.createMySQLTableSQL("writer", writerTableName, writerColumnsList, new ArrayList<>(), new ArrayList<>());
                jobDataContrastDetails.setWriterInfo(writerInfoMap.get("sqlText"));
                jobDataContrastDetails.setMetaTypeId("2");
                jobDataContrastDetailsMapper.save(jobDataContrastDetails);
            }
            //函数对比
            contrastPublicMySQL(taskId, jobDataContrast, SubMetaTypeEnum.FUNCTION.getMetaTypeId());
            //存过对比
            contrastPublicMySQL(taskId, jobDataContrast, SubMetaTypeEnum.PROCEDURE.getMetaTypeId());
            //视图对比
            contrastPublicMySQL(taskId, jobDataContrast, SubMetaTypeEnum.VIEW.getMetaTypeId());
            //触发器对比
            contrastPublicMySQL(taskId, jobDataContrast, SubMetaTypeEnum.TRIGGER.getMetaTypeId());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return ReturnT.SUCCESS;
    }

    /**
     * MySQL其它对象对比
     * @param taskId
     * @param jobDataContrast
     * @param metaTypeId
     */
    public void contrastPublicMySQL(int taskId, JobDataContrast jobDataContrast, int metaTypeId){
        //传输的参数
        Long readerDatasourceId = jobDataContrast.getReaderDatasourceId();
        String readerSchema = jobDataContrast.getReaderSchema();
        Long writerDatasourceId = jobDataContrast.getWriterDatasourceId();
        String writerSchema = jobDataContrast.getWriterSchema();
        //源端的对象集合
        List<Map<String, String>> readerList = subMetaDataMySQLService.generateComparePublicSQL(readerSchema, readerDatasourceId, metaTypeId);
        //触发器函数集合
        List<Map<String, String>> triggerFunctionList = new ArrayList<>();
        if(metaTypeId == SubMetaTypeEnum.TRIGGER.getMetaTypeId()){
            triggerFunctionList = subHaloDataService.generateComparePublicSQL(writerSchema, writerDatasourceId, -1);
        }
        //目标端的对象集合
        List<Map<String, String>> writerList = subHaloDataService.generateComparePublicSQL(writerSchema, writerDatasourceId, metaTypeId);
        for (Map<String, String> map:readerList) {
            String readerObjectName = map.get("OBJECT_NAME");
            //源端与目标端信息是否一致
            int isCorrect = 1;
            String writerObjectName= "";
            String writerSql = "";
            String mappingInfo = "";
            //查询目标端是否存在该对象
            List<Map<String, String>> filteredWriterList = writerList.stream().filter(s -> s.get("OBJECT_NAME").equalsIgnoreCase(readerObjectName)).collect(Collectors.toList());
            if(!filteredWriterList.isEmpty()){
                writerObjectName = filteredWriterList.get(0).get("OBJECT_NAME");
                writerSql = filteredWriterList.get(0).get("SQL");
                String finalWriterObjectName = writerObjectName;
                writerList.removeIf(i -> i.get("OBJECT_NAME").equals(finalWriterObjectName));
                if(metaTypeId == SubMetaTypeEnum.FUNCTION.getMetaTypeId()){
                    //对比前将空格全部忽略
                    //String updateReaderSql = map.get("SQL").replaceAll(" ", "").replaceAll("\"", "");
                    //String updateWriterSql = cutString(writerSql, "AS \\$function\\$", "\\$function\\$").replaceAll(" ", "").replaceAll("\"", "");
                    //对比
                    if(readerObjectName.equalsIgnoreCase(writerObjectName)){
                        isCorrect = 0;
                    }
                    mappingInfo = writerSql.replaceAll(" " + writerSchema + "\\.", " ")
                    .replaceAll(" OR REPLACE", "")
                    .replaceAll("\n LANGUAGE plmyssql", "")
                    .replaceAll("AS \\$function\\$", "")
                    .replaceAll("\\$function\\$", "");
                } else if(metaTypeId == SubMetaTypeEnum.PROCEDURE.getMetaTypeId()){
                    //对比
                    if(readerObjectName.equalsIgnoreCase(writerObjectName)){
                        isCorrect = 0;
                    }
                    mappingInfo = writerSql.replaceAll(" " + writerSchema + "\\.", " ")
                    .replaceAll(" OR REPLACE", "")
                    .replaceAll("\n LANGUAGE plmyssql", "")
                    .replaceAll("AS \\$procedure\\$", "")
                    .replaceAll("\\$procedure\\$", "");
                } else if(metaTypeId == SubMetaTypeEnum.VIEW.getMetaTypeId()){
                    //对比
                    if(readerObjectName.equalsIgnoreCase(writerObjectName)){
                        isCorrect = 0;
                    }
                    writerSql = "CREATE VIEW "+writerSchema+"."+writerObjectName+" AS" +writerSql;
                    mappingInfo = writerSql;
                } else if(metaTypeId == SubMetaTypeEnum.TRIGGER.getMetaTypeId()){
                    //对比
                    if(readerObjectName.equalsIgnoreCase(writerObjectName)){
                        isCorrect = 0;
                    }
                    //找出触发器的函数
                    List<Map<String, String>> filteredTriggerFunctionList = triggerFunctionList.stream().filter(s -> s.get("OBJECT_NAME").equalsIgnoreCase(readerObjectName+"_function")).collect(Collectors.toList());
                    if(!filteredTriggerFunctionList.isEmpty()){
                        String triggerFunctionSql = filteredTriggerFunctionList.get(0).get("SQL");
                        triggerFunctionSql = triggerFunctionSql.replaceAll(" " + writerSchema + "\\.", " ")
                        .replaceAll(" OR REPLACE", "")
                        .replaceAll("\n LANGUAGE plmyssql", "")
                        .replaceAll("AS \\$function\\$", "")
                        .replaceAll("\\$function\\$", "");
                        writerSql = writerSql.replaceAll(" " + writerSchema + "\\.", " ");
                        writerSql = triggerFunctionSql + "\n" +writerSql;
                    }
                    mappingInfo = writerSql;
                }
            }
            //保存详情
            JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
            jobDataContrastDetails.setTaskId(taskId);
            jobDataContrastDetails.setReaderTable(readerObjectName);
            jobDataContrastDetails.setReaderRecordRows(0L);
            jobDataContrastDetails.setWriterTable(writerObjectName);
            jobDataContrastDetails.setWriterRecordRows(0L);
            jobDataContrastDetails.setIsCorrect(isCorrect);
            jobDataContrastDetails.setReaderInfo(map.get("SQL"));
            jobDataContrastDetails.setWriterInfo(writerSql);
            jobDataContrastDetails.setMappingInfo(mappingInfo);
            jobDataContrastDetails.setMetaTypeId(String.valueOf(metaTypeId));
            jobDataContrastDetailsMapper.save(jobDataContrastDetails);
        }
        //处理目标端剩下的对象
        for (Map<String, String> map:writerList) {
            String writerObjectName = map.get("OBJECT_NAME");
            String writerSql = map.get("SQL");
            //保存详情
            JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
            jobDataContrastDetails.setTaskId(taskId);
            jobDataContrastDetails.setReaderTable("");
            jobDataContrastDetails.setReaderRecordRows(0L);
            jobDataContrastDetails.setWriterTable(writerObjectName);
            jobDataContrastDetails.setWriterRecordRows(0L);
            jobDataContrastDetails.setIsCorrect(1);
            jobDataContrastDetails.setReaderInfo("");
            if(metaTypeId == SubMetaTypeEnum.VIEW.getMetaTypeId()){
                writerSql = "CREATE VIEW "+writerSchema+"."+writerObjectName+" AS" +writerSql;
            }
            jobDataContrastDetails.setWriterInfo(writerSql);
            jobDataContrastDetails.setMappingInfo("");
            jobDataContrastDetails.setMetaTypeId(String.valueOf(metaTypeId));
            jobDataContrastDetailsMapper.save(jobDataContrastDetails);
        }
    }

    /**
     * 设置Oracle源端与目标端对比的list
     * @param typeName
     * @param list
     * @return
     */
    public List<Map<String, String>> convertingOracleColumnsList(String typeName, List<Map<String, String>> list){
        for (Map<String,String> map : list) {
            // 都变成小写
            map.put("COLUMN_NAME", map.get("COLUMN_NAME").toLowerCase());
            // 将数据类型长度都设置字符空,相当于不参与对比
            map.put("DATA_LENGTH", "");
            map.put("IS_NULLABLE", "");
            map.put("COLUMN_DEFAULT", "");
        }
        return list;
    }

    /**
     * 设置MySQL源端与目标端对比的list
     * @param typeName
     * @param list
     * @return
     */
    public List<Map<String, String>> convertingMySQLColumnsList(String typeName, List<Map<String, String>> list){
        for (Map<String,String> map : list) {
            // 将源端的字段名变小写
            if (typeName.equals("reader")){
                map.put("COLUMN_NAME", map.get("COLUMN_NAME").toLowerCase());
            }
            // 对默认值做对比处理
            if(map.get("COLUMN_DEFAULT") != null){
                String COLUMN_DEFAULT = map.get("COLUMN_DEFAULT");
                if (typeName.equals("reader")){
                    // 遇到CURRENT_TIMESTAMP去掉单引号,和halo一致
                    if(COLUMN_DEFAULT.contains("CURRENT_TIMESTAMP")){
                        COLUMN_DEFAULT = COLUMN_DEFAULT.replaceAll("'","");
                    } else{
                        COLUMN_DEFAULT = "'" + COLUMN_DEFAULT + "'";
                    }
                } else{
                    //遇到int加上单引号,和MySQL一致
                    if (map.get("DATA_TYPE").equals("int")){
                        if(COLUMN_DEFAULT.contains("'")){
                            //以防本身就有单引号
                        } else{
                            COLUMN_DEFAULT = "'" + COLUMN_DEFAULT + "'";
                        }
                    }
                    //遇到bit(1)则改为以下方式,和MySQL一致
                    if (map.get("DATA_TYPE").equals("bit") && map.get("DATA_LENGTH").equals("1")){
                        COLUMN_DEFAULT = "'b'0''";
                    }
                    //暂对mysql内核迁移做兼容对比
                    if(COLUMN_DEFAULT.contains("LOCALTIMESTAMP")){
                        //mysql不会加上(0)
                        if(COLUMN_DEFAULT.equals("LOCALTIMESTAMP(0)")){
                            COLUMN_DEFAULT = "CURRENT_TIMESTAMP";
                        } else{
                            //LOCALTIMESTAMP(3) - CURRENT_TIMESTAMP(3)
                            COLUMN_DEFAULT = COLUMN_DEFAULT.replaceAll("LOCALTIMESTAMP", "CURRENT_TIMESTAMP");
                        }
                    }
                    if(COLUMN_DEFAULT.contains("NULL") || COLUMN_DEFAULT.contains("nextval")){
                        // 删除最后的空格
                        COLUMN_DEFAULT = null;
                    } else{
                        //将::后面包括自身的所有内容全部删除
                        COLUMN_DEFAULT = COLUMN_DEFAULT.replaceAll("::.*", "");
                    }
                    //将::后面包括自身的所有内容全部删除
                    //COLUMN_DEFAULT = COLUMN_DEFAULT.replaceAll("::.*", "");
                }
                map.put("COLUMN_DEFAULT", COLUMN_DEFAULT);
            }
            // 将数据类型长度都设置字符空,相当于不参与对比
            map.put("DATA_LENGTH", "");
        }
        return list;
    }

    @Override
    public Map<String, Object> pageList(int start, int length, String taskName, String metaType) {
        // page list
        List<JobDataContrast> list = jobDataContrastMapper.pageList(start, length, taskName, metaType);
        int list_count = jobDataContrastMapper.pageListCount(start, length, taskName, metaType);
        // package result
        Map<String, Object> maps = new HashMap<>();
        maps.put("recordsTotal", list_count);        // 总记录数
        maps.put("recordsFiltered", list_count);    // 过滤后的总记录数
        maps.put("data", list);                    // 分页列表
        return maps;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public ReturnT<String> delete(int id) {
        jobDataContrastMapper.delete(id);
        jobDataContrastDetailsMapper.deleteByTaskId(id);
        return ReturnT.SUCCESS;
    }

    @Override
    public Map<String, Object> pageDetailsList(int start, int length, int taskId, String tableName, int isCorrect, int fastQue, String metaTypeId) {
        // page list
        List<JobDataContrastDetails> list = jobDataContrastDetailsMapper.pageList(start, length, taskId, tableName, isCorrect, fastQue, metaTypeId);
        int list_count = jobDataContrastDetailsMapper.pageListCount(start, length, taskId, tableName, isCorrect, fastQue, metaTypeId);
        // package result
        Map<String, Object> maps = new HashMap<>();
        maps.put("recordsTotal", list_count);        // 总记录数
        maps.put("recordsFiltered", list_count);    // 过滤后的总记录数
        maps.put("data", list);                    // 分页列表
        return maps;
    }

    @Override
    public Map<String, Object> detailsCount(int taskId, String metaTypeId) {
        return jobDataContrastDetailsMapper.detailsCount(taskId, metaTypeId);
    }

    /**
     * 约束对比
     * @param taskId
     * @param jobDataContrast
     */
    public void contrastConstraint(int taskId, JobDataContrast jobDataContrast){
        //传输的参数
        Long readerDatasourceId = jobDataContrast.getReaderDatasourceId();
        String readerSchema = jobDataContrast.getReaderSchema();
        Long writerDatasourceId = jobDataContrast.getWriterDatasourceId();
        String writerSchema = jobDataContrast.getWriterSchema();
        //源端的约束集合
        List<Map<String, String>> readerList = subMetaDataOracleService.generateCompareConstraintSQL(readerSchema, readerDatasourceId);
        //目标端的约束集合
        List<Map<String, String>> writerList = subHaloDataService.generateCompareConstraintSQL(writerSchema, writerDatasourceId);
        for (Map<String, String> map:readerList) {
            String readerObjectName = map.get("CONSTRAINT_NAME");
            //源端表字段信息与目标端表字段信息是否一致
            int isCorrect = 1;
            String writerObjectName= "";
            String writerSql = "";
            //查询目标端是否存在该对象
            List<Map<String, String>> filteredWriterList = writerList.stream().filter(s -> s.get("CONSTRAINT_NAME").equalsIgnoreCase(readerObjectName)).collect(Collectors.toList());
            if(filteredWriterList.isEmpty()){
                filteredWriterList = writerList.stream().filter(s -> s.get("CONSTRAINT_NAME").equalsIgnoreCase(readerObjectName + "_HALO")).collect(Collectors.toList());
            }
            if(!filteredWriterList.isEmpty()){
                writerObjectName = filteredWriterList.get(0).get("CONSTRAINT_NAME");
                writerSql = filteredWriterList.get(0).get("SQL");
                
                // 找到writerObjectName中最后一个_halo的位置
                int lastHaloConstraintInObjectName = writerObjectName.lastIndexOf("_halo");
                if (lastHaloConstraintInObjectName >= 0) {
                    // 获取去掉最后一个_halo后缀后的名称
                    String baseName = writerObjectName.substring(0, lastHaloConstraintInObjectName);
                    // 查找writerObjectName在writerSql中的位置
                    int haloConstraintIndex = writerSql.lastIndexOf(writerObjectName);
                    // 如果找到
                    if (haloConstraintIndex >= 0) {
                        writerSql = writerSql.replace(writerObjectName, baseName);
                    }
                }

                String finalWriterObjectName = writerObjectName;
                writerList.removeIf(i -> i.get("CONSTRAINT_NAME").equals(finalWriterObjectName));
                //对比前将空格全部忽略
                String updateReaderSql = map.get("SQL").replaceAll(" ", "").replaceAll("\\(", "").replaceAll("\\)", "");
                String updateWriterSql = writerSql.replaceAll(" ", "")
                                                .replaceAll("'", "")
                                                .replaceAll("\\(", "")
                                                .replaceAll("\\)", "")
                                                .replaceAll("::integer", "")
                                                .replaceAll("::numeric", "");
                                                // .replaceAll("_halo", "");
                //对比
                if(updateReaderSql.equalsIgnoreCase(updateWriterSql)){
                    isCorrect = 0;
                }
            }
            //保存详情
            JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
            jobDataContrastDetails.setTaskId(taskId);
            jobDataContrastDetails.setReaderTable(readerObjectName);
            jobDataContrastDetails.setReaderRecordRows(0L);
            jobDataContrastDetails.setWriterTable(writerObjectName);
            jobDataContrastDetails.setWriterRecordRows(0L);
            jobDataContrastDetails.setIsCorrect(isCorrect);
            jobDataContrastDetails.setReaderInfo(map.get("SQL"));
            jobDataContrastDetails.setWriterInfo(writerSql);
            jobDataContrastDetails.setMappingInfo("");
            jobDataContrastDetails.setMetaTypeId("3");
            jobDataContrastDetailsMapper.save(jobDataContrastDetails);
        }
        //处理目标端剩下的约束
        for (Map<String, String> map:writerList) {
            String writerObjectName = map.get("CONSTRAINT_NAME");
            String writerSql = map.get("SQL");
            //保存详情
            JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
            jobDataContrastDetails.setTaskId(taskId);
            jobDataContrastDetails.setReaderTable("");
            jobDataContrastDetails.setReaderRecordRows(0L);
            jobDataContrastDetails.setWriterTable(writerObjectName);
            jobDataContrastDetails.setWriterRecordRows(0L);
            jobDataContrastDetails.setIsCorrect(1);
            jobDataContrastDetails.setReaderInfo("");
            jobDataContrastDetails.setWriterInfo(writerSql);
            jobDataContrastDetails.setMappingInfo("");
            jobDataContrastDetails.setMetaTypeId("3");
            jobDataContrastDetailsMapper.save(jobDataContrastDetails);
        }
    }

    /**
     * 索引对比
     * @param taskId
     * @param jobDataContrast
     */
    public void contrastIndex(int taskId, JobDataContrast jobDataContrast){
        //传输的参数
        Long readerDatasourceId = jobDataContrast.getReaderDatasourceId();
        String readerSchema = jobDataContrast.getReaderSchema();
        Long writerDatasourceId = jobDataContrast.getWriterDatasourceId();
        String writerSchema = jobDataContrast.getWriterSchema();
        //源端的索引集合
        List<Map<String, String>> readerList = subMetaDataOracleService.generateCompareIndexSQL(readerSchema, readerDatasourceId);
        //目标端的索引集合
        List<Map<String, String>> writerList = subHaloDataService.generateCompareIndexSQL(writerSchema, writerDatasourceId);
        for (Map<String, String> map:readerList) {
            String readerObjectName = map.get("INDEX_NAME");
            //源端与目标端信息是否一致
            int isCorrect = 1;
            String writerObjectName= "";
            String writerSql = "";
            //查询目标端是否存在该对象
            List<Map<String, String>> filteredWriterList = writerList.stream().filter(s -> s.get("INDEX_NAME").equalsIgnoreCase(readerObjectName)).collect(Collectors.toList());
            if(filteredWriterList.isEmpty()){
                filteredWriterList = writerList.stream().filter(s -> s.get("INDEX_NAME").equalsIgnoreCase(readerObjectName + "_HALO")).collect(Collectors.toList());
            }
            if(!filteredWriterList.isEmpty()){
                writerObjectName = filteredWriterList.get(0).get("INDEX_NAME");
                writerSql = filteredWriterList.get(0).get("SQL");

                // 找到writerObjectName中最后一个_halo的位置
                int lastHaloIndexInObjectName = writerObjectName.lastIndexOf("_halo");
                if (lastHaloIndexInObjectName >= 0) {
                    // 获取去掉最后一个_halo后缀后的名称
                    String baseName = writerObjectName.substring(0, lastHaloIndexInObjectName);
                    // 查找writerObjectName在writerSql中的位置
                    int haloIndex = writerSql.lastIndexOf(writerObjectName);
                    // 如果找到
                    if (haloIndex >= 0) {
                        writerSql = writerSql.replace(writerObjectName, baseName);
                    }
                }

                String finalWriterObjectName = writerObjectName;
                writerList.removeIf(i -> i.get("INDEX_NAME").equals(finalWriterObjectName));
                //对比前将空格全部忽略
                String updateReaderSql = map.get("SQL").replaceAll(" ", "").replaceAll("\"", "");
                String updateWriterSql = writerSql.replaceAll(" USING btree", "")
                                                  .replaceAll(" ", "")
                                                  .replaceAll("\"", "");
                                                //   .replaceAll("_halo", "");
                //对比
                if(updateReaderSql.equalsIgnoreCase(updateWriterSql)){
                    isCorrect = 0;
                }
            }
            //保存详情
            JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
            jobDataContrastDetails.setTaskId(taskId);
            jobDataContrastDetails.setReaderTable(readerObjectName);
            jobDataContrastDetails.setReaderRecordRows(0L);
            jobDataContrastDetails.setWriterTable(writerObjectName);
            jobDataContrastDetails.setWriterRecordRows(0L);
            jobDataContrastDetails.setIsCorrect(isCorrect);
            jobDataContrastDetails.setReaderInfo(map.get("SQL"));
            jobDataContrastDetails.setWriterInfo(writerSql);
            jobDataContrastDetails.setMappingInfo("");
            jobDataContrastDetails.setMetaTypeId("4");
            jobDataContrastDetailsMapper.save(jobDataContrastDetails);
        }
        //处理目标端剩下的索引
        for (Map<String, String> map:writerList) {
            String writerObjectName = map.get("INDEX_NAME");
            String writerSql = map.get("SQL");
            //保存详情
            JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
            jobDataContrastDetails.setTaskId(taskId);
            jobDataContrastDetails.setReaderTable("");
            jobDataContrastDetails.setReaderRecordRows(0L);
            jobDataContrastDetails.setWriterTable(writerObjectName);
            jobDataContrastDetails.setWriterRecordRows(0L);
            jobDataContrastDetails.setIsCorrect(1);
            jobDataContrastDetails.setReaderInfo("");
            jobDataContrastDetails.setWriterInfo(writerSql);
            jobDataContrastDetails.setMappingInfo("");
            jobDataContrastDetails.setMetaTypeId("4");
            jobDataContrastDetailsMapper.save(jobDataContrastDetails);
        }
    }

    public void contrastPublic(int taskId, JobDataContrast jobDataContrast, int metaTypeId){
        //传输的参数
        Long readerDatasourceId = jobDataContrast.getReaderDatasourceId();
        String readerSchema = jobDataContrast.getReaderSchema();
        Long writerDatasourceId = jobDataContrast.getWriterDatasourceId();
        String writerSchema = jobDataContrast.getWriterSchema();
        //源端的对象集合
        List<Map<String, String>> readerList = subMetaDataOracleService.generateComparePublicSQL(readerSchema, readerDatasourceId, metaTypeId);
        //目标端的对象集合
        List<Map<String, String>> writerList = subHaloDataService.generateComparePublicSQL(writerSchema, writerDatasourceId, metaTypeId);
        //触发器函数集合
        List<Map<String, String>> triggerFunctionList = new ArrayList<>();
        if(metaTypeId == SubMetaTypeEnum.TRIGGER.getMetaTypeId()){
            triggerFunctionList = subHaloDataService.generateComparePublicSQL(writerSchema, writerDatasourceId, -1);
        }
        //自定义类型函数集合，用于后续过滤，不参与函数的对比
        List<Map<String, String>> typeList = new ArrayList<>();
        if(metaTypeId == SubMetaTypeEnum.FUNCTION.getMetaTypeId()){
            typeList = subHaloDataService.generateComparePublicSQL(writerSchema, writerDatasourceId, SubMetaTypeEnum.TYPE.getMetaTypeId());
        }
        for (Map<String, String> map:readerList) {
            String readerObjectName = map.get("OBJECT_NAME");
            //源端与目标端信息是否一致
            int isCorrect = 1;
            String writerObjectName= "";
            String writerSql = "";
            String mappingInfo = "";
            //查询目标端是否存在该对象
            List<Map<String, String>> filteredWriterList = writerList.stream().filter(s -> s.get("OBJECT_NAME").equalsIgnoreCase(readerObjectName)).collect(Collectors.toList());
            if(!filteredWriterList.isEmpty()){
                writerObjectName = filteredWriterList.get(0).get("OBJECT_NAME");
                writerSql = filteredWriterList.get(0).get("SQL");
                String finalWriterObjectName = writerObjectName;
                writerList.removeIf(i -> i.get("OBJECT_NAME").equals(finalWriterObjectName));
                if(metaTypeId == SubMetaTypeEnum.FUNCTION.getMetaTypeId()){
                    //对比前将空格全部忽略
                    // String updateReaderSql = map.get("SQL").replaceAll(" ", "").replaceAll("\"", "");
                    // String updateWriterSql = cutString(writerSql, "AS \\$function\\$", "\\$function\\$").replaceAll(" ", "").replaceAll("\"", "");
                    // //对比
                    // if(updateReaderSql.contains(updateWriterSql)){
                    //     isCorrect = 0;
                    // }
                    // 根据名称对比
                    if(readerObjectName.equalsIgnoreCase(writerObjectName)){
                        isCorrect = 0;
                    }
                    mappingInfo = cutString(writerSql, "AS \\$function\\$", "\\$function\\$");
                } else if(metaTypeId == SubMetaTypeEnum.PROCEDURE.getMetaTypeId()){
                    //对比前将空格全部忽略
                    // String updateReaderSql = map.get("SQL").replaceAll(" ", "").replaceAll("\"", "");
                    // String updateWriterSql = cutString(writerSql, "AS \\$procedure\\$", "\\$procedure\\$").replaceAll(" ", "").replaceAll("\"", "");
                    // //对比
                    // if(updateReaderSql.contains(updateWriterSql)){
                    //     isCorrect = 0;
                    // }
                    // 根据名称对比
                    if(readerObjectName.equalsIgnoreCase(writerObjectName)){
                        isCorrect = 0;
                    }
                    mappingInfo = cutString(writerSql, "AS \\$procedure\\$", "\\$procedure\\$");
                } else if(metaTypeId == SubMetaTypeEnum.VIEW.getMetaTypeId()){
                    mappingInfo = filteredWriterList.get(0).get("USEDDEFINITION");
                    //对比前将空格全部忽略
                    // String updateReaderSql = map.get("SQL").replaceAll(" ", "").replaceAll("\"", "").replaceAll(";", "");
                    // String updateWriterSql = mappingInfo.replaceAll(" ", "").replaceAll("\"", "").replaceAll(";", "");
                    // //对比
                    // if(updateReaderSql.contains(updateWriterSql)){
                    //     isCorrect = 0;
                    // }
                    // 根据名称对比
                    if(readerObjectName.equalsIgnoreCase(writerObjectName)){
                        isCorrect = 0;
                    }
                    writerSql = "CREATE OR REPLACE VIEW "+writerSchema+"."+writerObjectName+" AS" +writerSql;
                } else if(metaTypeId == SubMetaTypeEnum.SEQUENCE.getMetaTypeId()){
                    //对比前将空格全部忽略
                    String updateReaderSql = map.get("SQL").replaceAll(" NOORDER", "")
                            .replaceAll(" NOCACHE", "")
                            .replaceAll(" ORDER", "")
                            .replaceAll(" NOCYCLE", "")
                            .replaceAll(" NOKEEP", "")
                            .replaceAll(" NOSCALE", "")
                            .replaceAll(" GLOBAL", "")
                            .replaceAll(" NOPARTITION", "")
                            .replaceAll("\\d{18,}", "9223372036854775807")
                            .replaceAll(" ", "")
                            .replaceAll("\"", "");
                    String updateWriterSql = writerSql.replaceAll(" NO CYCLE", "")
                            //只删除CACHE以及前后都是非数字的1
                            .replaceAll(" CACHE (?<![0-9])1(?![0-9])", "")
                            .replaceAll(" ", "")
                            .replaceAll("\"", "");
                    //对比
                    if(updateReaderSql.equalsIgnoreCase(updateWriterSql)){
                        isCorrect = 0;
                    }
                } else if(metaTypeId == SubMetaTypeEnum.TRIGGER.getMetaTypeId()){
                    //找出触发器的函数
                    // 根据名称对比
                    if(readerObjectName.equalsIgnoreCase(writerObjectName)){
                        isCorrect = 0;
                    }
                    List<Map<String, String>> filteredTriggerFunctionList = triggerFunctionList.stream().filter(s -> s.get("OBJECT_NAME").equalsIgnoreCase(readerObjectName+"_function")).collect(Collectors.toList());
                    if(!filteredTriggerFunctionList.isEmpty()){
                        String triggerFunctionSql = filteredTriggerFunctionList.get(0).get("SQL");
                        //对比前将空格全部忽略
                        // String updateReaderSql = map.get("SQL").replaceAll(" ", "").replaceAll("\"", "").replaceAll(":", "").replaceAll("\n", "").replaceAll(";", "");
                        // String updateWriterSql = cutString(triggerFunctionSql.replaceAll("RETURN NEW;", "").replaceAll(" ", "").replaceAll("\"", "").replaceAll("\n", "").replaceAll(";", "").replaceAll(":", ""), "AS\\$function\\$", "\\$function\\$");
                        // //对比
                        // if(updateReaderSql.toLowerCase().contains(updateWriterSql.toLowerCase())){
                        //     isCorrect = 0;
                        // }
                        writerSql = writerSql + "\n" +triggerFunctionSql;
                    }
                } else if(metaTypeId == SubMetaTypeEnum.PACKAGE.getMetaTypeId()){
                    //对比前将空格全部忽略
                    // String updateReaderSql = map.get("SQL").replaceAll(" OR REPLACE EDITIONABLE", "").replaceAll(" ", "").replaceAll("\"", "");
                    // String updateWriterSql = writerSql.replaceAll(" ", "").replaceAll("\"", "");
                    // //对比
                    // if(updateReaderSql.contains(updateWriterSql)){
                    //     isCorrect = 0;
                    // }
                    // 根据名称对比
                    if(readerObjectName.equalsIgnoreCase(writerObjectName)){
                        isCorrect = 0;
                    }
                } else if(metaTypeId == SubMetaTypeEnum.TYPE.getMetaTypeId()){
                    isCorrect = 0;
                }
            }
            //保存详情
            JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
            jobDataContrastDetails.setTaskId(taskId);
            jobDataContrastDetails.setReaderTable(readerObjectName);
            jobDataContrastDetails.setReaderRecordRows(0L);
            jobDataContrastDetails.setWriterTable(writerObjectName);
            jobDataContrastDetails.setWriterRecordRows(0L);
            jobDataContrastDetails.setIsCorrect(isCorrect);
            jobDataContrastDetails.setReaderInfo(map.get("SQL"));
            jobDataContrastDetails.setWriterInfo(writerSql);
            jobDataContrastDetails.setMappingInfo(mappingInfo);
            jobDataContrastDetails.setMetaTypeId(String.valueOf(metaTypeId));
            jobDataContrastDetailsMapper.save(jobDataContrastDetails);
        }
        //处理目标端剩下的对象
        for (Map<String, String> map:writerList) {
            String writerObjectName = map.get("OBJECT_NAME");
            String writerSql = map.get("SQL");
            //自定义类型的函数不参与对比
            if(metaTypeId == SubMetaTypeEnum.FUNCTION.getMetaTypeId()){
                List<Map<String, String>> filteredTypeList = typeList.stream().filter(s -> s.get("OBJECT_NAME").equalsIgnoreCase(writerObjectName)).collect(Collectors.toList());
                if(!filteredTypeList.isEmpty()){
                    continue;
                }
            }
            //保存详情
            JobDataContrastDetails jobDataContrastDetails = new JobDataContrastDetails();
            jobDataContrastDetails.setTaskId(taskId);
            jobDataContrastDetails.setReaderTable("");
            jobDataContrastDetails.setReaderRecordRows(0L);
            jobDataContrastDetails.setWriterTable(writerObjectName);
            jobDataContrastDetails.setWriterRecordRows(0L);
            jobDataContrastDetails.setIsCorrect(1);
            jobDataContrastDetails.setReaderInfo("");
            jobDataContrastDetails.setWriterInfo(writerSql);
            jobDataContrastDetails.setMappingInfo("");
            jobDataContrastDetails.setMetaTypeId(String.valueOf(metaTypeId));
            jobDataContrastDetailsMapper.save(jobDataContrastDetails);
        }
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

    public String resultHaloTableName(String schema, String tableName){
        if(tableName == null || tableName.equals("")){
            return "";
        }
        if(SubMetaUtil.colNameIsNormal(tableName)){
            tableName = String.format("\"%s\"", tableName);
        }
        tableName = schema+"."+tableName;
        return tableName;
    }

    public String resultOracleTableName(String schema, String tableName){
        if(tableName == null || tableName.equals("")){
            return "";
        }
        if(SubMetaUtil.oraColNameIsNormal(tableName)){
            tableName = String.format("\"%s\"", tableName);
        }
        tableName = schema+"."+tableName;
        return tableName;
    }

    //resultMysqlTableName
    public String resultMysqlTableName(String schema, String tableName){
        if(tableName == null || tableName.equals("")){
            return "";
        }
        if(SubMetaUtil.oraColNameIsNormal(tableName)){
            tableName = String.format("`%s`", tableName);
        }
        tableName = schema+"."+tableName;
        return tableName;
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
     * 比较两个List<String>中的数据是否相同（不区分大小写）
     * @param list1
     * @param list2
     * @return
     */
    public static boolean areListsEqualIgnoreCase(List<String> list1, List<String> list2) {
        // 将列表转换为不区分大小写的HashSet
        Set<String> set1 = new HashSet<>(list1.size());
        for (String str : list1) {
            set1.add(str.toLowerCase());
        }
        Set<String> set2 = new HashSet<>(list2.size());
        for (String str : list2) {
            set2.add(str.toLowerCase());
        }
        // 比较两个HashSet是否相等
        return set1.equals(set2);
    }

}
