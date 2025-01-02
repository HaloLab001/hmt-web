package com.wugui.hmt.admin.service.impl;

import cn.hutool.core.util.ObjectUtil;
import com.google.common.collect.Lists;
import com.wugui.hmt.admin.entity.JobDatasource;
import com.wugui.hmt.admin.service.JobDatasourceService;
import com.wugui.hmt.admin.service.SubMetaDataHaloService;
import com.wugui.hmt.admin.tool.meta.SubHaloDatabaseMeta;
import com.wugui.hmt.admin.tool.query.BaseQueryTool;
import com.wugui.hmt.admin.tool.query.QueryToolFactory;
import com.wugui.hmt.admin.util.SubMetaTypeEnum;
import com.wugui.hmt.admin.util.SubMetaUtil;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/*-------------------------------------------------------------------------
 *
 * SubMetaDataHaloServiceImpl.java
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/service/impl/SubMetaDataHaloServiceImpl.java
 *
 *-----------------------------------------------
 */
@Transactional(rollbackFor = Exception.class)
@Service
public class SubMetaDataHaloServiceImpl implements SubMetaDataHaloService {

    @Resource
    private JobDatasourceService jobDatasourceService;

    @Override
    public List<Map<String, String>> generateCompareConstraintSQL(String haloSchemaName, Long datasourceId) {
        List<Map<String, String>> constraintList = new ArrayList<>();
        List<Map<String, String>> dataInfoList = getMetaDataUnification(datasourceId, SubHaloDatabaseMeta.getConstraintsTypeList(haloSchemaName));
        for (Map<String, String> map : dataInfoList) {
            String sqlText = "";
            String tableSchema = map.get("tableschema");
            String tableName = map.get("tablename");
            String constraintName = map.get("constraintname");
            String definition = map.get("definition");
            String contype = map.get("contype");
            //由Oracle迁移过来的会多一个括号，导致后续对比不一致
            if(contype.equals("c")){
                definition = definition.replaceAll("\\(\\(", "\\(").replaceAll("\\)\\)", "\\)");
            }
            sqlText = "ALTER TABLE \""+tableSchema+"\".\""+tableName+"\" ADD CONSTRAINT \""+constraintName+"\" "+definition+";";
            //处理语句中的特殊字符或中文,加上双引号并且转义
            sqlText = SubMetaUtil.disposeSqlColNameIsNormal(sqlText);
            Map<String, String> objMap = new HashMap<>();
            objMap.put("CONSTRAINT_NAME", constraintName);
            objMap.put("SQL", sqlText);
            constraintList.add(objMap);
        }
        return constraintList;
    }

    @Override
    public List<Map<String, String>> generateCompareIndexSQL(String haloSchemaName, Long datasourceId) {
        List<Map<String, String>> indexList = new ArrayList<>();
        List<Map<String, String>> dataInfoList = getMetaDataUnification(datasourceId, SubHaloDatabaseMeta.getIndexTypeList(haloSchemaName));
        for (Map<String, String> map : dataInfoList) {
            String indexName = map.get("indexname");
            String sqlText = map.get("definition");
            sqlText = sqlText.replaceAll(" "+indexName, " "+haloSchemaName+"."+indexName) + ";";
            Map<String, String> objMap = new HashMap<>();
            objMap.put("INDEX_NAME", indexName);
            objMap.put("SQL", sqlText);
            indexList.add(objMap);
        }
        return indexList;
    }

    @Override
    public List<Map<String, String>> generateComparePublicSQL(String haloSchemaName, Long datasourceId, int metaTypeId) {
        List<Map<String, String>> publicList = new ArrayList<>();
        String sql = "";
        if(metaTypeId == SubMetaTypeEnum.FUNCTION.getMetaTypeId()){
            sql = SubHaloDatabaseMeta.getFunctionTypeList(haloSchemaName);
        } else if(metaTypeId == SubMetaTypeEnum.PROCEDURE.getMetaTypeId()){
            sql = SubHaloDatabaseMeta.getProcedureTypeList(haloSchemaName);
        } else if(metaTypeId == SubMetaTypeEnum.VIEW.getMetaTypeId()){
            sql = SubHaloDatabaseMeta.getViewTypeList(haloSchemaName);
        } else if(metaTypeId == SubMetaTypeEnum.SEQUENCE.getMetaTypeId()){
            sql = SubHaloDatabaseMeta.getSequenceTypeList(haloSchemaName);
        } else if(metaTypeId == SubMetaTypeEnum.TRIGGER.getMetaTypeId()){
            sql = SubHaloDatabaseMeta.getTriggerTypeList(haloSchemaName);
        } else if(metaTypeId == SubMetaTypeEnum.TYPE.getMetaTypeId()){
            sql = SubHaloDatabaseMeta.getTypeList(haloSchemaName);
        } else if(metaTypeId == SubMetaTypeEnum.PACKAGE.getMetaTypeId()){
            sql = SubHaloDatabaseMeta.getPackageTypeList(haloSchemaName);
        } else if(metaTypeId == -1){
            sql = SubHaloDatabaseMeta.getTriggerFunctionTypeList(haloSchemaName);
        }
        List<Map<String, String>> dataInfoList = getMetaDataUnification(datasourceId, sql);
        for (Map<String, String> map : dataInfoList) {
            String objectName = map.get("proname");
            String sqlText = map.get("definition");
            Map<String, String> objMap = new HashMap<>();
            objMap.put("OBJECT_NAME", objectName);
            objMap.put("SQL", sqlText);
            if(metaTypeId == SubMetaTypeEnum.VIEW.getMetaTypeId()){
                String useddefinition = map.get("useddefinition");
                objMap.put("USEDDEFINITION", useddefinition);
            } else if(metaTypeId == SubMetaTypeEnum.TYPE.getMetaTypeId()){
                String type = map.get("type");
                objMap.put("TYPE", type);
                if(type.equals("2")){
                    sqlText = "CREATE DOMAIN "+haloSchemaName+"."+objectName+" AS " + sqlText + ";";
                    objMap.put("SQL", sqlText);
                }
            }
            publicList.add(objMap);
        }
        return publicList;
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

}
