package com.wugui.hmt.admin.tool.meta;

/*-------------------------------------------------------------------------
 *
 * SubMySQLDatabaseMeta.java
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/tool/meta/SubMySQLDatabaseMeta.java
 *
 *-----------------------------------------------
 */
public class SubMySQLDatabaseMeta {

    public static String getTableList(String schema) {
        return "SELECT TABLE_NAME,TABLE_COMMENT,CREATE_OPTIONS from INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '"+schema+"' AND TABLE_TYPE = 'BASE TABLE'; ";
    }

    public static String getColumnsListByTable(String schema, String tableName) {
        return "SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, COLUMN_COMMENT FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '"+schema+"' AND TABLE_NAME = '"+tableName+"' ORDER BY ORDINAL_POSITION; ";
    }

    public static String getPartMapListByTable(String schema, String tableName) {
        return "SELECT TABLE_NAME, PARTITION_ORDINAL_POSITION, PARTITION_NAME, PARTITION_DESCRIPTION, TABLESPACE_NAME, PARTITION_METHOD, PARTITION_EXPRESSION, SUBPARTITION_NAME, PARTITION_ORDINAL_POSITION, SUBPARTITION_METHOD, SUBPARTITION_EXPRESSION FROM INFORMATION_SCHEMA.PARTITIONS WHERE PARTITION_NAME IS NOT NULL AND TABLE_SCHEMA ='"+schema+"' AND TABLE_NAME = '"+tableName+"' ORDER BY TABLE_NAME,PARTITION_ORDINAL_POSITION;";
    }

    public static String getColumnsConstraintList(String schema) {
        //return "select TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,IS_NULLABLE,COLUMN_DEFAULT from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = '"+schema+"' and (IS_NULLABLE = 'NO' or COLUMN_DEFAULT is not null); ";
        return "select a.TABLE_SCHEMA,a.TABLE_NAME,a.COLUMN_NAME,a.COLUMN_TYPE,a.IS_NULLABLE,a.COLUMN_DEFAULT,a.EXTRA from INFORMATION_SCHEMA.COLUMNS a INNER JOIN INFORMATION_SCHEMA.TABLES b on a.TABLE_NAME = b.TABLE_NAME AND b.TABLE_SCHEMA = '"+schema+"' AND b.TABLE_TYPE = 'BASE TABLE' where a.TABLE_SCHEMA = '"+schema+"' and (a.IS_NULLABLE = 'NO' or a.COLUMN_DEFAULT is not null) ;";
    }

    public static String getColumnsConstraintTypePList(String schema) {
        return "select a.TABLE_SCHEMA,a.TABLE_NAME,a.COLUMN_NAME,a.COLUMN_KEY,k.ORDINAL_POSITION from INFORMATION_SCHEMA.COLUMNS a INNER JOIN INFORMATION_SCHEMA.TABLES b on a.TABLE_NAME = b.TABLE_NAME AND b.TABLE_SCHEMA = '"+schema+"' AND b.TABLE_TYPE = 'BASE TABLE' INNER JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE k ON a.TABLE_SCHEMA = k.TABLE_SCHEMA AND a.TABLE_NAME = k.TABLE_NAME AND a.COLUMN_NAME = k.COLUMN_NAME AND k.CONSTRAINT_NAME = 'PRIMARY' where a.TABLE_SCHEMA = '"+schema+"' and a.COLUMN_KEY = 'PRI' ORDER BY a.TABLE_NAME, k.ORDINAL_POSITION; ";
    }

    public static String getCheckConstraintList(String schema) {
        return "SELECT DISTINCT TABLE_NAME FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_TYPE = 'CHECK' AND TABLE_SCHEMA = '"+schema+"' ";
    }

    public static String getIndexList(String schema) {
        return "SELECT TABLE_NAME, COLUMN_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, INDEX_TYPE FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '"+schema+"' AND INDEX_NAME != 'PRIMARY' ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX; ";
    }

    public static String getIndexListByTableName(String schema, String tableName) {
        return "SELECT TABLE_NAME, COLUMN_NAME, INDEX_NAME, NON_UNIQUE, SEQ_IN_INDEX, INDEX_TYPE FROM INFORMATION_SCHEMA.STATISTICS WHERE TABLE_SCHEMA = '"+schema+"' AND TABLE_NAME = '"+tableName+"' ORDER BY NON_UNIQUE, INDEX_NAME, SEQ_IN_INDEX; ";
    }

    public static String getForeignKeyList(String schema) {
        return "SELECT DISTINCT A.COLUMN_NAME,A.ORDINAL_POSITION,A.TABLE_NAME,A.REFERENCED_TABLE_NAME,A.REFERENCED_COLUMN_NAME,A.POSITION_IN_UNIQUE_CONSTRAINT,A.CONSTRAINT_NAME,A.REFERENCED_TABLE_SCHEMA,B.MATCH_OPTION,B.UPDATE_RULE,B.DELETE_RULE FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS A INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS B ON A.CONSTRAINT_NAME = B.CONSTRAINT_NAME AND A.CONSTRAINT_SCHEMA = B.CONSTRAINT_SCHEMA WHERE A.REFERENCED_COLUMN_NAME IS NOT NULL AND A.CONSTRAINT_SCHEMA='"+schema+"' ORDER BY A.ORDINAL_POSITION,A.POSITION_IN_UNIQUE_CONSTRAINT; ";
    }

    public static String getForeignKeyListByTableName(String schema, String tableName) {
        return "SELECT DISTINCT A.COLUMN_NAME,A.ORDINAL_POSITION,A.TABLE_NAME,A.REFERENCED_TABLE_NAME,A.REFERENCED_COLUMN_NAME,A.POSITION_IN_UNIQUE_CONSTRAINT,A.CONSTRAINT_NAME,A.REFERENCED_TABLE_SCHEMA,B.MATCH_OPTION,B.UPDATE_RULE,B.DELETE_RULE FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS A INNER JOIN INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS AS B ON A.CONSTRAINT_NAME = B.CONSTRAINT_NAME AND A.CONSTRAINT_SCHEMA = B.CONSTRAINT_SCHEMA WHERE A.REFERENCED_COLUMN_NAME IS NOT NULL AND A.CONSTRAINT_SCHEMA='"+schema+"' AND A.TABLE_NAME = '"+tableName+"' ORDER BY A.ORDINAL_POSITION,A.POSITION_IN_UNIQUE_CONSTRAINT; ";
    }

    public static String getFunctionList(String schema) {
        return "SELECT ROUTINE_NAME,ROUTINE_DEFINITION,ROUTINE_TYPE FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = '"+schema+"' AND ROUTINE_TYPE = 'FUNCTION' ORDER BY ROUTINE_NAME; ";
    }

    public static String getFunctionInfoByName(String schema, String objectName) {
        return "show create function `"+schema+"`.`"+objectName+"`; ";
    }

    public static String getProcedureList(String schema) {
        return "SELECT ROUTINE_NAME,ROUTINE_DEFINITION,ROUTINE_TYPE FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_SCHEMA = '"+schema+"' AND ROUTINE_TYPE = 'PROCEDURE' ORDER BY ROUTINE_NAME; ";
    }

    public static String getProcedureInfoByName(String schema, String objectName) {
        return "show create procedure `"+schema+"`.`"+objectName+"`; ";
    }

    public static String getViewList(String schema) {
        return "SELECT TABLE_NAME,VIEW_DEFINITION,CHECK_OPTION,IS_UPDATABLE,DEFINER,SECURITY_TYPE FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '"+schema+"' ORDER BY TABLE_NAME; ";
    }

    public static String getViewListByShow(String schema, String objectName) {
        return "show create view `"+schema+"`.`"+objectName+"`;  ";
    }

    public static String getAutoIncrementList(String schema) {
        return "SELECT a.TABLE_NAME,a.AUTO_INCREMENT,b.COLUMN_NAME,b.COLUMN_TYPE,b.EXTRA FROM INFORMATION_SCHEMA.TABLES a INNER JOIN INFORMATION_SCHEMA.COLUMNS b on a.TABLE_NAME = b.TABLE_NAME AND b.TABLE_SCHEMA = '"+schema+"' AND b.EXTRA = 'auto_increment' WHERE a.TABLE_TYPE='BASE TABLE' AND a.TABLE_SCHEMA = '"+schema+"' AND a.AUTO_INCREMENT IS NOT NULL; ";
    }

    public static String getTriggerList(String schema) {
        return "SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA = '"+schema+"' ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME; ";
    }

    public static String getTableInfoByName(String schema, String objectName) {
        return "show create table `"+schema+"`.`"+objectName+"`; ";
    }

}
