package com.wugui.hmt.admin.tool.meta;

import com.wugui.hmt.admin.tool.pojo.MetaTypePojo;
import com.wugui.hmt.admin.util.SubMetaEnum;
import com.wugui.hmt.admin.util.SubMetaTypeEnum;

/*-------------------------------------------------------------------------
 *
 * SubOracleDatabaseMeta.java
 *  MySQL元数据相关sql语句
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/tool/meta/SubOracleDatabaseMeta.java
 *
 *-----------------------------------------------
 */
public class SubOracleDatabaseMeta {

    public static String getPagePublicSqlInfo(String schema, MetaTypePojo metaTypePojo, int beginCount, int endCount, String selectAndSql) {
        //通用where条件
        String sqlWhere = "WHERE u.OWNER = '"+schema+"' AND U.OBJECT_TYPE IN ( '"+metaTypePojo.getObjectType()+"' ) AND U.OBJECT_NAME NOT LIKE 'BIN$%' ";
        //增加条件
        if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.SEQUENCE.getMetaTypeId()){
            sqlWhere = sqlWhere + " AND U.OBJECT_NAME not like '%ISEQ$$%' ";
        } else if (metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.TYPE.getMetaTypeId()){
            sqlWhere = sqlWhere + " AND U.OBJECT_NAME not like 'SYS_PLSQL_%' ";
        }
        //临时增加一个条件语句，用于过滤导致报错的信息，示例: AND U.OBJECT_NAME not like 'SYS_IOI_%'
        if(selectAndSql != null && !selectAndSql.equals("")){
            sqlWhere = sqlWhere + selectAndSql + " ";
        }
        //通用GET_DDL
        String sqlText = "";
        // String sqlText = "SELECT DBMS_METADATA.GET_DDL ( OBJECTTYPE, OBJECTNAME, '"+schema+"' ) AS sqlInfo,OBJECTNAME,PARTITIONING_TYPE,SUBPARTITIONING_TYPE FROM " +
        //         "(SELECT u.object_name AS OBJECTNAME,u.OBJECT_TYPE AS OBJECTTYPE,p.PARTITIONING_TYPE,p.SUBPARTITIONING_TYPE,ROW_NUMBER() OVER (ORDER BY u.OBJECT_ID) ro FROM DBA_OBJECTS u LEFT JOIN ALL_PART_TABLES p ON  U.OWNER = p.OWNER AND U.object_name = p.TABLE_NAME " + sqlWhere +
        //         ") WHERE ro BETWEEN "+beginCount+" AND "+endCount+" ";
        if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.TABLE.getMetaTypeId()){
            sqlText = "SELECT DBMS_METADATA.GET_DDL ( OBJECTTYPE, OBJECTNAME, '"+schema+"' ) AS sqlInfo,OBJECTNAME,PARTITIONING_TYPE,SUBPARTITIONING_TYPE,TEMPORARY FROM " +
                "(SELECT u.object_name AS OBJECTNAME,u.OBJECT_TYPE AS OBJECTTYPE,u.TEMPORARY AS TEMPORARY,p.PARTITIONING_TYPE,p.SUBPARTITIONING_TYPE,ROW_NUMBER() OVER (ORDER BY u.OBJECT_ID) ro FROM DBA_OBJECTS u LEFT JOIN ALL_PART_TABLES p ON  U.OWNER = p.OWNER AND U.object_name = p.TABLE_NAME " + sqlWhere +
                ") WHERE ro BETWEEN "+beginCount+" AND "+endCount+" ";
        } else{
            sqlText = "SELECT DBMS_METADATA.GET_DDL ( OBJECTTYPE, OBJECTNAME, '"+schema+"' ) AS sqlInfo,OBJECTNAME,'' as PARTITIONING_TYPE,'' as SUBPARTITIONING_TYPE,TEMPORARY as TEMPORARY FROM " +
                "(SELECT u.object_name AS OBJECTNAME,u.OBJECT_TYPE AS OBJECTTYPE,u.TEMPORARY AS TEMPORARY,ROW_NUMBER() OVER (ORDER BY u.OBJECT_ID) ro FROM DBA_OBJECTS u " + sqlWhere +
                ") WHERE ro BETWEEN "+beginCount+" AND "+endCount+" ";
        }
        return sqlText;
    }

    public static String setPublicTransformParam(MetaTypePojo metaTypePojo) {
        boolean CONSTRAINTS_AS_ALTER = true;
        boolean CONSTRAINTS = false;
        boolean REF_CONSTRAINTS = false;
        boolean SQLTERMINATOR = false;
        //为查询表结构变更参数
        if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.TABLE.getMetaTypeId()){
            CONSTRAINTS = false;
            REF_CONSTRAINTS = false;
            SQLTERMINATOR = true;
            CONSTRAINTS_AS_ALTER = false;
        } else if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.INDEX.getMetaTypeId()){
            SQLTERMINATOR = true;
        }
        String sqlText =
                "begin\n" +
                "-- 如果TRUE，则在 DDL 中包含段属性子句（物理属性、存储属性、表空间、日志记录）。如果FALSE，省略它们。默认为TRUE.\n" +
                "dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'SEGMENT_ATTRIBUTES', false);\n" +
                "-- 如果TRUE，则在 DDL 中包含存储子句。如果FALSE，省略它们。（如果SEGMENT_ATTRIBUTES是则忽略FALSE。）默认为TRUE.\n" +
                "dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'STORAGE', false);\n" +
                "-- 如果TRUE，则在 DDL 中包含表空间子句。如果FALSE，省略它们。（如果SEGMENT_ATTRIBUTES是则忽略FALSE。）默认为TRUE.\n" +
                "dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'TABLESPACE', false);\n" +
                "dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'SIZE_BYTE_KEYWORD', false);\n" +
                "-- 如果TRUE，将表约束包含为单独的ALTER TABLE（并且，如果需要，CREATE INDEX）语句。如果FALSE，将表约束指定为CREATE TABLE语句的一部分。默认为 FALSE. 要求CONSTRAINTS be TRUE.\n" +
                "dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'CONSTRAINTS_AS_ALTER', "+CONSTRAINTS_AS_ALTER+");\n" +
                "-- 如果TRUE，则在输出 SXML 中包含所有非引用表约束。如果FALSE，省略它们。默认为TRUE.\n" +
                "dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'CONSTRAINTS', "+CONSTRAINTS+");\n" +
                "-- 如果TRUE，则在 DDL 中包含所有引用约束（外键）。如果FALSE，省略它们。默认为TRUE.\n" +
                "dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'REF_CONSTRAINTS', "+REF_CONSTRAINTS+");\n" +
                "-- 如果TRUE，则将 SQL 终止符（;或/）附加到每个 DDL 语句。默认为FALSE.\n" +
                "dbms_metadata.set_transform_param(dbms_metadata.session_transform, 'SQLTERMINATOR', "+SQLTERMINATOR+");\n" +
                "end;";
        return sqlText;
    }

    public static String getPublicMetaCount(String schema, MetaTypePojo metaTypePojo, String selectAndSql){
        String sqlText = "SELECT COUNT(*) FROM DBA_OBJECTS u WHERE u.owner = '"+schema+"'  AND u.OBJECT_TYPE IN ( '"+metaTypePojo.getObjectType()+"' ) AND U.OBJECT_NAME NOT LIKE 'BIN$%' ";
        //增加条件
        if(metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.SEQUENCE.getMetaTypeId()){
            sqlText = sqlText + " AND U.OBJECT_NAME not like '%ISEQ$$%' ";
        } else if (metaTypePojo.getMetaTypeId() == SubMetaTypeEnum.TYPE.getMetaTypeId()){
            sqlText = sqlText + " AND U.OBJECT_NAME not like 'SYS_PLSQL_%' ";
        }
        //临时增加一个条件语句，用于过滤导致报错的信息，示例: AND U.OBJECT_NAME not like 'SYS_IOI_%'
        if(selectAndSql != null && !selectAndSql.equals("")){
            sqlText = sqlText + selectAndSql + " ";
        }
        return sqlText;
    }

    public static String getUserConstraintsIndexList(String schema){
        return "select CONSTRAINT_NAME AS sqlInfo,1 AS objectName  from ALL_CONSTRAINTS WHERE owner = '"+schema+"' AND TABLE_NAME NOT LIKE 'BIN$%' AND INDEX_NAME IS NOT NULL";
    }

    public static String getMetaConstraintsTypeCCount(String schema){
        String sqlText = "SELECT COUNT(*) FROM ALL_CONSTRAINTS A WHERE A.OWNER = '"+schema+"' AND A.CONSTRAINT_TYPE='C' AND A.TABLE_NAME NOT LIKE 'BIN$%' ";
        return sqlText;
    }

    public static String getConstraintsTypeCList(String schema, int beginCount, int endCount){
        return " SELECT * FROM (" +
               "   SELECT" +
               "   A.CONSTRAINT_NAME," +
               "   A.R_CONSTRAINT_NAME," +
               "   A.SEARCH_CONDITION," +
               "   A.DELETE_RULE," +
               "   A.DEFERRABLE," +
               "   A.DEFERRED," +
               "   A.R_OWNER," +
               "   A.TABLE_NAME," +
               "   A.OWNER," +
               "   A.VALIDATED," +
               "   B.TEMPORARY," +
               "   ROW_NUMBER() OVER (ORDER BY A.CONSTRAINT_NAME) ro" +
               "   FROM ALL_CONSTRAINTS A JOIN DBA_TABLES B ON (A.TABLE_NAME = B.TABLE_NAME AND B.OWNER = '"+schema+"')" +
               "   WHERE A.OWNER = '"+schema+"' AND A.CONSTRAINT_TYPE='C' AND A.TABLE_NAME NOT LIKE 'BIN$%' " +
               " ) WHERE ro BETWEEN "+beginCount+" AND "+endCount+" ";
    }

    public static String getConstraintsTypePList(String schema){
        return " SELECT DISTINCT " +
                " A.COLUMN_NAME," +
                " A.CONSTRAINT_NAME," +
                " A.OWNER," +
                " A.POSITION," +
                " B.CONSTRAINT_NAME," +
                " B.CONSTRAINT_TYPE," +
                " B.DEFERRABLE," +
                " B.DEFERRED," +
                " B.GENERATED," +
                " B.TABLE_NAME," +
                " B.OWNER," +
                " C.TEMPORARY" +
                " FROM ALL_CONS_COLUMNS A JOIN ALL_CONSTRAINTS B ON (B.CONSTRAINT_NAME = A.CONSTRAINT_NAME AND B.OWNER = A.OWNER)" +
                " JOIN DBA_TABLES C ON (A.TABLE_NAME = C.TABLE_NAME AND C.OWNER = '"+schema+"')" +
                " WHERE B.OWNER = '"+schema+"'" +
                " AND B.CONSTRAINT_TYPE = 'P' AND A.TABLE_NAME NOT LIKE 'BIN$%'" +
                " ORDER BY B.TABLE_NAME, B.CONSTRAINT_NAME, A.POSITION";
    }

    public static String getConstraintsTypeRList(String schema){
        return " SELECT" +
               "    CONS.TABLE_NAME," +
               "    CONS.CONSTRAINT_NAME," +
               "    COLS.COLUMN_NAME," +
               "    CONS_R.TABLE_NAME R_TABLE_NAME," +
               "    CONS.R_CONSTRAINT_NAME," +
               "    COLS_R.COLUMN_NAME R_COLUMN_NAME," +
               "    CONS.SEARCH_CONDITION,CONS.DELETE_RULE,CONS.DEFERRED," +
               "    CONS.OWNER,CONS.R_OWNER," +
               "    COLS.POSITION,COLS_R.POSITION," +
               "    CONS.VALIDATED" +
               " FROM ALL_CONSTRAINTS CONS" +
               "    LEFT JOIN ALL_CONS_COLUMNS COLS ON (COLS.CONSTRAINT_NAME = CONS.CONSTRAINT_NAME AND COLS.OWNER = CONS.OWNER AND COLS.TABLE_NAME = CONS.TABLE_NAME)" +
               "    LEFT JOIN ALL_CONSTRAINTS CONS_R ON (CONS_R.CONSTRAINT_NAME = CONS.R_CONSTRAINT_NAME AND CONS_R.OWNER = CONS.R_OWNER)" +
               "    LEFT JOIN ALL_CONS_COLUMNS COLS_R ON (COLS_R.CONSTRAINT_NAME = CONS.R_CONSTRAINT_NAME AND COLS_R.POSITION=COLS.POSITION AND COLS_R.OWNER = CONS.R_OWNER)" +
               " WHERE CONS.OWNER = '"+schema+"' AND CONS.CONSTRAINT_TYPE = 'R' AND CONS.TABLE_NAME NOT LIKE 'BIN$%' ORDER BY CONS.TABLE_NAME, CONS.CONSTRAINT_NAME, COLS.POSITION";
    }

    public static String getAllTabComments(String schema, String tableName, int tabType){
        String sqlText = "SELECT * FROM ALL_TAB_COMMENTS WHERE OWNER = '"+schema+"' AND COMMENTS IS NOT NULL AND TABLE_NAME = '"+tableName+"' ";
        if(tabType == SubMetaEnum.TABLE_TYPE_1.getTypeId()){
            sqlText = sqlText + " AND TABLE_TYPE = 'TABLE' ";
        } else if(tabType == SubMetaEnum.TABLE_TYPE_2.getTypeId()){
            sqlText = sqlText + " AND TABLE_TYPE = 'VIEW' ";
        }
        return sqlText;
    }

    public static String getAllColCommentsList(String schema, String tableName){
        return "SELECT * FROM ALL_COL_COMMENTS WHERE OWNER = '"+schema+"' AND COMMENTS IS NOT NULL AND TABLE_NAME = '"+tableName+"' ";
    }

    public static String getMetaTriggerCount(String schema){
        return "SELECT COUNT(*) FROM All_TRIGGERS WHERE OWNER = '"+schema+"' ";
    }

    public static String getPageTriggerSqlInfo(String schema, int beginCount, int endCount) {
        return "SELECT * FROM" +
                "(SELECT TRIGGER_NAME,TRIGGER_TYPE,TRIGGERING_EVENT,TABLE_NAME,TRIGGER_BODY,WHEN_CLAUSE,DESCRIPTION,ACTION_TYPE,OWNER,ROW_NUMBER ( ) OVER ( ORDER BY TRIGGER_NAME ) ro " +
                "FROM All_TRIGGERS WHERE OWNER = '"+schema+"' ORDER BY TABLE_NAME,TRIGGER_NAME " +
                ") WHERE ro BETWEEN "+beginCount+" AND "+endCount+" ";
    }

    public static String getPartTablesList(String schema, String tableName, int partType){
        //默认主分区信息
        String tableColText = "A.PARTITION_POSITION,A.PARTITION_NAME,B.PARTITIONING_TYPE";
        String partTableName = "ALL_TAB_PARTITIONS A, ALL_PART_TABLES B, ALL_PART_KEY_COLUMNS";
        String orderByText = " A.TABLE_OWNER,A.TABLE_NAME,A.PARTITION_POSITION,C.COLUMN_POSITION";
        //子分区信息
        if(partType == SubMetaEnum.PART_TABLE_2.getTypeId()){
            tableColText = "A.SUBPARTITION_POSITION,A.SUBPARTITION_NAME,A.PARTITION_NAME,B.SUBPARTITIONING_TYPE";
            partTableName = "ALL_TAB_SUBPARTITIONS A, ALL_PART_TABLES B, ALL_SUBPART_KEY_COLUMNS";
            orderByText = " A.TABLE_OWNER,A.TABLE_NAME,A.PARTITION_NAME,A.SUBPARTITION_POSITION,C.COLUMN_POSITION";
        }
        return "SELECT" +
                " A.TABLE_NAME," +
                " A.HIGH_VALUE," +
                " A.TABLESPACE_NAME," +
                " C.NAME," +
                " C.COLUMN_NAME," +
                " C.COLUMN_POSITION," +
                " A.TABLE_OWNER," +
                 tableColText +
                " FROM "+partTableName+" C" +
                " WHERE" +
                " a.table_name = b.table_name AND" +
                " (b.partitioning_type = 'RANGE' OR b.partitioning_type = 'LIST' OR b.partitioning_type = 'HASH')" +
                " AND a.table_name = c.name" +
                " AND A.TABLE_OWNER ='"+schema+"' AND B.OWNER=A.TABLE_OWNER AND C.OWNER=A.TABLE_OWNER" +
                " AND A.TABLE_NAME = '"+tableName+"'" +
                " ORDER BY " + orderByText;
    }

    public static String getExistBigTypeTables(String schema, String tableName){
        String sqlText = "SELECT distinct table_name FROM ALL_TAB_COLUMNS WHERE OWNER = '"+schema+"' ";
        if(!tableName.equals("")){
            sqlText = sqlText + "AND TABLE_NAME = '"+tableName+"' ";
        }
        sqlText = sqlText + "AND DATA_TYPE IN ('BLOB','CLOB','NCLOB','RAW') ";
        return sqlText;
    }

    public static String getConstraintsTypeCListNotPage(String schema){
        return "    SELECT" +
                "   A.CONSTRAINT_NAME," +
                "   A.R_CONSTRAINT_NAME," +
                "   A.SEARCH_CONDITION," +
                "   A.DELETE_RULE," +
                "   A.DEFERRABLE," +
                "   A.DEFERRED," +
                "   A.R_OWNER," +
                "   A.TABLE_NAME," +
                "   A.OWNER," +
                "   A.VALIDATED," +
                "   ROW_NUMBER() OVER (ORDER BY A.CONSTRAINT_NAME) ro" +
                "   FROM ALL_CONSTRAINTS A WHERE A.OWNER = '"+schema+"' AND A.CONSTRAINT_TYPE='C' AND A.TABLE_NAME NOT LIKE 'BIN$%' ";
    }

}
