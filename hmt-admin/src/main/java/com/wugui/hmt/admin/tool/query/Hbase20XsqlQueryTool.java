package com.wugui.hmt.admin.tool.query;

import com.google.common.collect.Lists;
import com.wugui.hmt.admin.core.util.LocalCacheUtil;
import com.wugui.hmt.admin.entity.JobDatasource;
import com.wugui.hmt.admin.util.JdbcUtils;


import java.sql.*;
import java.util.ArrayList;
import java.util.List;


/*-------------------------------------------------------------------------
 *
 * Hbase20XsqlQueryTool.java
 *  for HBase2.X and Phoenix5.X数据库使用的查询工具
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/tool/query/Hbase20XsqlQueryTool.java
 *
 *-----------------------------------------------
 */
public class Hbase20XsqlQueryTool extends BaseQueryTool implements QueryToolInterface {
    Connection conn = null;


    /**
     * 构造方法
     *
     * @param jobJdbcDatasource
     */
    public Hbase20XsqlQueryTool(JobDatasource jobJdbcDatasource) throws SQLException {
        super(jobJdbcDatasource);

        if (LocalCacheUtil.get(jobJdbcDatasource.getDatasourceName()) == null) {
            getDataSource(jobJdbcDatasource);
        } else {
            conn = (Connection) LocalCacheUtil.get(jobJdbcDatasource.getDatasourceName());
            if (conn == null) {
                LocalCacheUtil.remove(jobJdbcDatasource.getDatasourceName());
            }
        }
        LocalCacheUtil.set(jobJdbcDatasource.getDatasourceName(), conn, 4 * 60 * 60 * 1000);

    }

    @Override
    public List<String> getTableNames(String tableSchema) {
        DatabaseMetaData metaData = null;
        List<String> tables = new ArrayList<String>();
        ResultSet rs = null;
        try {
            metaData = conn.getMetaData();
            rs = metaData.getTables(conn.getCatalog(), null, "%", new String[]{"TABLE"});
            while (rs.next()) {
                tables.add(rs.getString("TABLE_NAME"));
            }

        } catch (SQLException e) {
            logger.error("[getTableNames Exception] --> "
                    + "the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(rs);
        }


        return tables;
    }

    @Override
    public List<String> getColumnNames(String tableSchema, String tableName, String datasource) {
        DatabaseMetaData metaData = null;
        List<String> columnNames = Lists.newArrayList();
        ResultSet rs = null;
        try {
            metaData = conn.getMetaData();
            rs = metaData.getColumns(conn.getCatalog(), null, tableName, "%");
            while (rs.next()) {
                columnNames.add(rs.getString("COLUMN_NAME"));
                // 获取字段的数据类型  rs.getString("TYPE_NAME")
            }

        } catch (SQLException e) {
            logger.error("[getColumnNames Exception] --> "
                    + "the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(rs);
        }


        return columnNames;
    }


    private static int getSize(ResultSet rs) {
        try {
            if (rs.getType() == ResultSet.TYPE_FORWARD_ONLY) {
                return -1;
            }

            rs.last();
            int total = rs.getRow();
            rs.beforeFirst();
            return total;
        } catch (SQLException sqle) {
            return -1;
        } catch (AbstractMethodError ame) {
            return -1;
        }
    }


    private void getDataSource(JobDatasource jobDatasource) throws SQLException {
        conn = DriverManager.getConnection(jobDatasource.getJdbcUrl());


    }


}
