package com.wugui.hmt.admin.tool.meta;

/*-------------------------------------------------------------------------
 *
 * HaloDatabaseMeta.java
 *  Halo数据库 meta信息查询
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/tool/meta/HaloDatabaseMeta.java
 *
 *-----------------------------------------------
 */
public class HaloDatabaseMeta extends BaseDatabaseMeta implements DatabaseInterface {

    private volatile static HaloDatabaseMeta single;

    public static HaloDatabaseMeta getInstance() {
        if (single == null) {
            synchronized (HaloDatabaseMeta.class) {
                if (single == null) {
                    single = new HaloDatabaseMeta();
                }
            }
        }
        return single;
    }

    @Override
    public String getSQLQueryPrimaryKey() {
        return "select column_name from information_schema.columns where table_schema='public' and table_name='tb_cis_patient_info' and is_identity = 'YES'";
    }

    @Override
    public String getSQLQueryTables() {
        return "select relname as tabname from pg_class c \n" +
                "where  relkind = 'r' and relname not like 'pg_%' and relname not like 'sql_%' group by relname order by relname limit 500";
    }


    @Override
    public String getSQLQueryTables(String... tableSchema) {
        // return "SELECT concat_ws('.',a.table_schema,a.table_name) FROM information_schema.tables a " +
        //         "LEFT JOIN pg_catalog.pg_class b on a.table_name = b.relname " +
        //         "LEFT JOIN pg_catalog.pg_namespace c ON c.oid = b.relnamespace " +
        //         "where (a.table_name not like 'pg_%' OR a.table_name not like 'sql_%') " +
        //         "and a.table_type='BASE TABLE' and b.relispartition = false and a.table_schema='" + tableSchema[0] + "' and c.nspname = '" + tableSchema[0] + "'";
        //过滤分区表
        return "SELECT concat_ws('.',a.table_schema,a.table_name) FROM information_schema.tables a " +
                "LEFT JOIN pg_catalog.pg_class b on a.table_name = b.relname " +
                "LEFT JOIN pg_catalog.pg_namespace c ON c.oid = b.relnamespace " +
                "WHERE a.table_type='BASE TABLE' and b.relispartition = false and a.table_schema='" + tableSchema[0] + "' and c.nspname = '" + tableSchema[0] + "'";
    }

    @Override
    public String getSQLQueryTableSchema(String... args) {
        // return "select table_schema FROM information_schema.tables where \"table_name\" not like 'pg_%' or \"table_name\" not like 'sql_%' group by table_schema order by table_schema;";
        return "SELECT schema_name as table_schema FROM information_schema.schemata where schema_name not in ('pg_catalog', 'pg_toast', 'information_schema') order by schema_name;";
    }

    @Override
    public String getSQLQueryColumns(String... args) {
        return "SELECT a.attname as name \n" +
                "FROM pg_class as c,pg_attribute as a where c.relname = ? and a.attrelid = c.oid and a.attnum>0";
    }

    @Override
    public String getSQLQueryComment(String schemaName, String tableName, String columnName) {
        return null;
    }

    @Override
    public String getSQLQueryNoPrimaryKeyTables(String... tableSchema) {
        return "SELECT DISTINCT A.TABLE_NAME FROM INFORMATION_SCHEMA.TABLES A " +
                "LEFT JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS B ON A.TABLE_NAME = B.TABLE_NAME " +
                "AND B.TABLE_SCHEMA = '"+tableSchema[0]+"' AND B.CONSTRAINT_TYPE = 'PRIMARY KEY' " +
                "WHERE A.TABLE_TYPE='BASE TABLE' AND A.TABLE_SCHEMA='"+tableSchema[0]+"' AND B.CONSTRAINT_TYPE IS NULL ORDER BY A.TABLE_NAME ";
    }

    @Override
    public String getSQLQueryTablesNoSchema(String tableSchema) {
        return "SELECT a.table_name FROM information_schema.tables a " +
                "LEFT JOIN pg_catalog.pg_class b on a.table_name = b.relname " +
                "LEFT JOIN pg_catalog.pg_namespace c ON c.oid = b.relnamespace " +
                "WHERE a.table_type='BASE TABLE' and b.relispartition = false and a.table_schema='" + tableSchema + "' and c.nspname = '" + tableSchema + "'";
    }

    @Override
    public String getTableRows(String tableName) {
        return "SELECT COUNT(*) AS tableRows from "+tableName+" ";
    }

    @Override
    public String getTableColumnsAndType(String tableSchema, String tableName) {
        return "SELECT column_name as COLUMN_NAME,data_type as DATA_TYPE,character_maximum_length as DATA_LENGTH,is_nullable as IS_NULLABLE, column_default as COLUMN_DEFAULT FROM information_schema.columns WHERE table_schema = '"+tableSchema+"' AND table_name = '"+tableName+"' order by ordinal_position ";
    }

}
