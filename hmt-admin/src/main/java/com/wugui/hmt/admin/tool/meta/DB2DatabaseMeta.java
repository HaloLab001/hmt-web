package com.wugui.hmt.admin.tool.meta;


/*-------------------------------------------------------------------------
 *
 * DB2DatabaseMeta.java
 *  DB2数据库meta信息查询
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/tool/meta/DB2DatabaseMeta.java
 *
 *-----------------------------------------------
 */
public class DB2DatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {
    private volatile static DB2DatabaseMeta single;

    public static DB2DatabaseMeta getInstance() {
        if (single == null) {
            synchronized (DB2DatabaseMeta.class) {
                if (single == null) {
                    single = new DB2DatabaseMeta();
                }
            }
        }
        return single;
    }

    @Override
    public String getSQLQueryTablesNameComments() {
        return "SELECT TABNAME, REMARKS FROM SYSCAT.TABLES WHERE TABSCHEMA = ?";
    }

    @Override
    public String getSQLQueryTableNameComment() {
        return "SELECT TABNAME, REMARKS FROM SYSCAT.TABLES WHERE TABSCHEMA = ? AND TABNAME = ?";
    }

    @Override
    public String getSQLQueryPrimaryKey() {
        return "SELECT COLNAME FROM SYSCAT.KEYCOLUSE WHERE TABSCHEMA = ? AND TABNAME = ?";
    }

    @Override
    public String getSQLQueryComment(String schemaName, String tableName, String columnName) {
        return String.format("SELECT REMARKS FROM SYSCAT.COLUMNS WHERE TABSCHEMA = '%s' AND TABNAME = '%s' AND COLNAME = '%s'", schemaName, tableName, columnName);
    }

    @Override
    public String getSQLQueryNoPrimaryKeyTables(String... tableSchema) {
        return "SELECT tabname as TABLE_NAME FROM syscat.tables " +
                        " WHERE not exists ( " +
                        "   SELECT 1 FROM syscat.indexes WHERE syscat.tables.tabname = syscat.indexes.tabname " +
                        "   AND syscat.tables.tabschema = syscat.indexes.tabschema " +
                        "   AND uniquerule = 'P') " +
                        " AND tabschema = '"+tableSchema[0]+"'";
    }

    @Override
    public String getSQLQueryTablesNoSchema(String tableSchema) {
        return null;
    }

    @Override
    public String getTableRows(String tableName) {
        return null;
    }

    @Override
    public String getTableColumnsAndType(String tableSchema, String tableName) {
        return null;
    }

    @Override
    public String getSQLQueryColumns(String... args) {
        return "SELECT COLNAME FROM SYSCAT.COLUMNS WHERE TABSCHEMA = ? AND TABNAME = ?";
    }

    @Override
    public String getSQLQueryTableSchema(String... args) {
        return "SELECT SCHEMANAME FROM SYSCAT.SCHEMATA WHERE SCHEMANAME NOT IN('NULLID','ROOT','SQLJ','SYSCAT','SYSFUN','SYSIBM','SYSIBMADM','SYSIBMINTERNAL','SYSIBMTS','SYSPROC','SYSPUBLIC','SYSSTAT','SYSTOOLS')";
    }

    @Override
    public String getSQLQueryTables() {
        return "SELECT CONCAT(TABSCHEMA,CONCAT('.',TABNAME)) AS TABNAME, REMARKS FROM SYSCAT.TABLES WHERE TABSCHEMA = CURRENT SCHEMA";
    }

    @Override
    public String getSQLQueryTables(String... tableSchema) {
        return String.format("SELECT CONCAT(TABSCHEMA,CONCAT('.',TABNAME)) AS TABNAME, REMARKS FROM SYSCAT.TABLES WHERE TABSCHEMA = '%s' AND TYPE = 'T' ", tableSchema[0]);
    }

}