package com.wugui.hmt.admin.tool.query;

import java.sql.SQLException;

import com.wugui.hmt.admin.entity.JobDatasource;
import com.wugui.hmt.admin.util.JdbcConstants;
import com.wugui.hmt.admin.util.RdbmsException;


/*-------------------------------------------------------------------------
 *
 * QueryToolFactory.java
 *  工具类，获取单例实体
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/tool/query/QueryToolFactory.java
 *
 *-----------------------------------------------
 */
public class QueryToolFactory {

    public static BaseQueryTool getByDbType(JobDatasource jobDatasource) {
        //获取dbType
        String datasource = jobDatasource.getDatasource();
        if (JdbcConstants.MYSQL.equals(datasource)) {
            return getMySQLQueryToolInstance(jobDatasource);
        } else if (JdbcConstants.ORACLE.equals(datasource)) {
            return getOracleQueryToolInstance(jobDatasource);
        } else if (JdbcConstants.POSTGRESQL.equals(datasource)) {
            return getPostgresqlQueryToolInstance(jobDatasource);
        } else if (JdbcConstants.SQL_SERVER.equals(datasource)) {
            return getSqlserverQueryToolInstance(jobDatasource);
        }else if (JdbcConstants.HIVE.equals(datasource)) {
            return getHiveQueryToolInstance(jobDatasource);
        } else if (JdbcConstants.CLICKHOUSE.equals(datasource)) {
            return getClickHouseQueryToolInstance(jobDatasource);
        }else if (JdbcConstants.HBASE20XSQL.equals(datasource)) {
            return getHbase20XsqlQueryToolQueryToolInstance(jobDatasource);
        }else if (JdbcConstants.DB2.equals(datasource)) {
            return getDB2QueryToolInstance(jobDatasource);
        }else if (JdbcConstants.HALO.equals(datasource)) {
            return getHaloQueryToolInstance(jobDatasource);
        }else if (JdbcConstants.DM.equals(datasource)) {
            return getDmQueryToolInstance(jobDatasource);
        }
        throw new UnsupportedOperationException("找不到该类型: ".concat(datasource));
    }

    private static BaseQueryTool getMySQLQueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new MySQLQueryTool(jdbcDatasource);
        } catch (Exception e) {
            throw RdbmsException.asConnException(JdbcConstants.MYSQL,
                e,jdbcDatasource.getJdbcUsername(),jdbcDatasource.getDatasourceName());
        }
    }

    private static BaseQueryTool getOracleQueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new OracleQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw RdbmsException.asConnException(JdbcConstants.ORACLE,
                e,jdbcDatasource.getJdbcUsername(),jdbcDatasource.getDatasourceName());
        }
    }

    private static BaseQueryTool getPostgresqlQueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new PostgresqlQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw RdbmsException.asConnException(JdbcConstants.POSTGRESQL,
                e,jdbcDatasource.getJdbcUsername(),jdbcDatasource.getDatasourceName());
        }
    }

    private static BaseQueryTool getSqlserverQueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new HaloQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw RdbmsException.asConnException(JdbcConstants.SQL_SERVER,
                e,jdbcDatasource.getJdbcUsername(),jdbcDatasource.getDatasourceName());
        }
    }

    private static BaseQueryTool getHiveQueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new HiveQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw RdbmsException.asConnException(JdbcConstants.HIVE,
                e,jdbcDatasource.getJdbcUsername(),jdbcDatasource.getDatasourceName());
        }
    }
    private static BaseQueryTool getClickHouseQueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new ClickHouseQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw RdbmsException.asConnException(JdbcConstants.CLICKHOUSE,
                e, jdbcDatasource.getJdbcUsername(), jdbcDatasource.getDatasourceName());
        }
    }

    private static Hbase20XsqlQueryTool getHbase20XsqlQueryToolQueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new Hbase20XsqlQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw RdbmsException.asConnException(JdbcConstants.HBASE20XSQL,
                e, jdbcDatasource.getJdbcUsername(), jdbcDatasource.getDatasourceName());
        }
    }

    private static BaseQueryTool getDB2QueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new DB2SQLQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw RdbmsException.asConnException(JdbcConstants.DB2,
                e, jdbcDatasource.getJdbcUsername(), jdbcDatasource.getDatasourceName());
        }
    }

    private static BaseQueryTool getHaloQueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new HaloQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw RdbmsException.asConnException(JdbcConstants.HALO,
                e, jdbcDatasource.getJdbcUsername(), jdbcDatasource.getDatasourceName());
        }
    }

    private static BaseQueryTool getDmQueryToolInstance(JobDatasource jdbcDatasource) {
        try {
            return new DmQueryTool(jdbcDatasource);
        } catch (SQLException e) {
            throw RdbmsException.asConnException(JdbcConstants.DM,
                    e, jdbcDatasource.getJdbcUsername(), jdbcDatasource.getDatasourceName());
        }
    }

}
