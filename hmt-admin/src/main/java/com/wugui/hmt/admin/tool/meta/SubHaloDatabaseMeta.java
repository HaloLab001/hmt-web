package com.wugui.hmt.admin.tool.meta;

/*-------------------------------------------------------------------------
 *
 * SubHaloDatabaseMeta.java
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/tool/meta/SubHaloDatabaseMeta.java
 *
 *-----------------------------------------------
 */
public class SubHaloDatabaseMeta {

    public static String getConstraintsTypeList(String schema){
        return " SELECT distinct on(constraintName) * FROM ( " +
                "    SELECT " +
                "       c.conname as constraintName, " +
                "       c.contype, " +
                "       pg_namespace.nspname as tableSchema, " +
                "       pg_class.relname as tableName, " +
                "       con.column_name as columnName, " +
                "       pg_get_constraintdef(c.oid) as definition " +
                "    FROM " +
                "        pg_constraint c " +
                "    JOIN " +
                "        pg_namespace ON pg_namespace.oid = c.connamespace " +
                "    JOIN " +
                "        pg_class ON c.conrelid = pg_class.oid " +
                "    LEFT JOIN " +
                "        information_schema.constraint_column_usage con ON " +
                "        c.conname = con.constraint_name AND pg_namespace.nspname = con.constraint_schema " +
                "    where table_schema = '"+schema+"'" +
                "    and pg_class.relispartition = FALSE" +  // 排除分区子表
                ") a ";
    }

    public static String getIndexTypeList(String schema){
        return  " SELECT" +
                " n.nspname AS schemaname," +
                "    c.relname AS tablename," +
                "    i.relname AS indexname," +
                "    t.spcname AS tablespace," +
                "    pg_get_indexdef(i.oid) AS definition" +
                " FROM pg_index x" +
                "    JOIN pg_class c ON c.oid = x.indrelid" +
                "    JOIN pg_class i ON i.oid = x.indexrelid" +
                "    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace" +
                "    LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace" +
                " WHERE (c.relkind = ANY (ARRAY['r'::\"char\", 'm'::\"char\"])) AND i.relkind = 'i'::\"char\" AND n.nspname = '"+schema+"'" +
                "   AND c.relispartition = FALSE;"; // 排除分区子表的索引
    }

    public static String getIndexTypeListByTableName(String schema, String tableName){
        return  " SELECT" +
                " x.indisunique," +
                " n.nspname AS schemaname," +
                "    c.relname AS tablename," +
                "    i.relname AS indexname," +
                "    t.spcname AS tablespace," +
                "    pg_get_indexdef(i.oid) AS definition" +
                " FROM pg_index x" +
                "    JOIN pg_class c ON c.oid = x.indrelid" +
                "    JOIN pg_class i ON i.oid = x.indexrelid" +
                "    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace" +
                "    LEFT JOIN pg_tablespace t ON t.oid = i.reltablespace" +
                " WHERE (c.relkind = ANY (ARRAY['r'::\"char\", 'm'::\"char\"])) AND i.relkind = 'i'::\"char\" AND n.nspname = '"+schema+"' AND c.relname = '"+tableName+"' order by x.indisunique DESC,i.relname ASC;";
    }

    public static String getFunctionTypeList(String schema){
        return  " SELECT p.proname,pg_get_functiondef(p.oid) AS definition" +
                " FROM pg_proc p" +
                " JOIN pg_namespace n ON (n.oid = p.pronamespace)" +
                " WHERE p.prokind = 'f' and n.nspname = '"+schema+"' and p.proname not like '%_function' ;";
    }

    public static String getProcedureTypeList(String schema){
        return  " SELECT p.proname,pg_get_functiondef(p.oid) AS definition" +
                " FROM pg_proc p" +
                " JOIN pg_namespace n ON (n.oid = p.pronamespace)" +
                " WHERE p.prokind = 'p' and n.nspname = '"+schema+"';";
    }

    public static String getViewTypeList(String schema){
        return  " SELECT c.relname as proname, pg_get_viewdef('\"' || n.nspname || '\".\"' || c.relname || '\"') AS definition, pg_get_viewdef(c.oid) AS useddefinition" +
                " FROM pg_class c" +
                " JOIN pg_namespace n ON n.oid = c.relnamespace" +
                " WHERE c.relkind = 'v' AND n.nspname = '"+schema+"'";
    }

    public static String getSequenceTypeList(String schema){
        return  " SELECT sequencename as proname, 'CREATE SEQUENCE ' || schemaname || '.' || sequencename || E'' ||" +
                "       ' MINVALUE ' || min_value || E'' ||" +
                "       ' MAXVALUE ' || max_value || E'' ||" +
                "       ' INCREMENT BY ' || increment_by || E'' ||" +
                "       ' START WITH ' || start_value || E'' ||" +
                "       ' CACHE ' || cache_size || E'' ||" +
                "       ' ' || CASE WHEN cycle THEN 'CYCLE' ELSE 'NO CYCLE' END || E'' || ';' AS definition" +
                " FROM pg_sequences" +
                " WHERE schemaname = '"+schema+"';";
    }

    public static String getTriggerTypeList(String schema){
        return  " SELECT tgname AS proname,pg_get_triggerdef(pg_trigger.oid) AS definition" +
                " FROM pg_trigger" +
                " JOIN pg_class ON pg_trigger.tgrelid = pg_class.oid" +
                " JOIN pg_namespace ON pg_class.relnamespace = pg_namespace.oid" +
                " WHERE nspname = '"+schema+"' and tgname not like '%RI_ConstraintTrigger%';";
    }

    public static String getTriggerFunctionTypeList(String schema){
        return  " SELECT p.proname,pg_get_functiondef(p.oid) AS definition" +
                " FROM pg_proc p" +
                " JOIN pg_namespace n ON (n.oid = p.pronamespace)" +
                " WHERE n.nspname = '"+schema+"' and p.proname like '%_function';";
    }

    public static String getPackageTypeList(String schema){
        return  " SELECT t.pkgname as proname, t.pkgspec as definition" +
                " FROM pg_package t" +
                " JOIN pg_namespace n ON n.oid = t.pkgnamespace " +
                " where n.nspname = '"+schema+"'";
    }

    public static String getTypeList(String schema){
        return  " SELECT pg_class.relname as proname, pg_get_functiondef((select oid from pg_proc where proname = pg_class.relname) ) as definition, '1' as type" +
                " FROM pg_class" +
                " JOIN pg_namespace ON pg_namespace.oid = pg_class.relnamespace" +
                " WHERE pg_namespace.nspname = '"+schema+"' AND pg_class.relkind = 'c'" +
                " UNION ALL" +
                " SELECT t.typname as proname, format_type(t.typbasetype, t.typtypmod) as definition, '2' as type" +
                " FROM pg_type t " +
                " JOIN pg_namespace n ON n.oid = t.typnamespace" +
                " where n.nspname = '"+schema+"' and t.typtype = 'd'";
    }

    public static String getForeignKeyListByTableName(String schema, String tableName){
        return  " SELECT" + 
                " conname AS constraint_name," + 
                " pg_get_constraintdef(pg_constraint.oid) AS constraint_definition," + 
                " tbl.relname AS table_name," + 
                " array_to_string(conkey, ', ') AS column_names," + 
                " ftbl.relname AS foreign_table_name," + 
                " array_to_string(confkey, ', ') AS foreign_column_names" + 
                " FROM" + 
                " pg_constraint" + 
                " INNER JOIN" + 
                " pg_class tbl ON pg_constraint.conrelid = tbl.oid" + 
                " INNER JOIN" + 
                " pg_namespace nsp ON nsp.oid = tbl.relnamespace" + 
                " INNER JOIN" + 
                " pg_class ftbl ON pg_constraint.confrelid = ftbl.oid" + 
                " WHERE" + 
                " pg_constraint.contype = 'f'" + 
                " AND nsp.nspname = '"+schema+"'" + 
                " AND tbl.relname = '"+tableName+"';";
    }

}
