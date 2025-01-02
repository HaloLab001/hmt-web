package com.wugui.hmt.admin.util;


/*-------------------------------------------------------------------------
 *
 * JdbcConstants.java
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/util/JdbcConstants.java
 *
 *-----------------------------------------------
 */
public interface JdbcConstants {


    String HBASE_ZK_QUORUM              = "hbase.zookeeper.quorum";

    String MONGODB                      ="mongodb";

    String JTDS                       = "jtds";

    String MOCK                       = "mock";

    String HSQL                       = "hsql";

    String DB2                        = "db2";

    String DB2_DRIVER                 = "com.ibm.db2.jcc.DB2Driver";

    String POSTGRESQL                 = "postgresql";
    String POSTGRESQL_DRIVER          = "org.postgresql.Driver";

    String HALO                       = "halo";
    String HALO_DRIVER                = "com.halo.Driver";

    String SYBASE                     = "sybase";

    String SQL_SERVER                 = "sqlserver";
    String SQL_SERVER_DRIVER          = "com.microsoft.jdbc.sqlserver.SQLServerDriver";
    String SQL_SERVER_DRIVER_SQLJDBC4 = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    String SQL_SERVER_DRIVER_JTDS     = "net.sourceforge.jtds.jdbc.Driver";

    String ORACLE                     = "oracle";
    String ORACLE_DRIVER              = "oracle.jdbc.OracleDriver";
    String ORACLE_DRIVER2             = "oracle.jdbc.driver.OracleDriver";

    String ALI_ORACLE                 = "AliOracle";
    String ALI_ORACLE_DRIVER          = "com.alibaba.jdbc.AlibabaDriver";

    String MYSQL                      = "mysql";
    String MYSQL_DRIVER               = "com.mysql.jdbc.Driver";
    String MYSQL_DRIVER_6             = "com.mysql.cj.jdbc.Driver";
    String MYSQL_DRIVER_REPLICATE     = "com.mysql.jdbc.";

    String MARIADB                    = "mariadb";
    String MARIADB_DRIVER             = "org.mariadb.jdbc.Driver";

    String DERBY                      = "derby";

    String HBASE                      = "hbase";

    String HIVE                       = "hive";
    String HIVE_DRIVER                = "org.apache.hive.jdbc.HiveDriver";

    String H2                         = "h2";
    String H2_DRIVER                  = "org.h2.Driver";

    String DM                         = "dm";
    String DM_DRIVER                  = "dm.jdbc.driver.DmDriver";

    String KINGBASE                   = "kingbase";
    String KINGBASE_DRIVER            = "com.kingbase.Driver";

    String GBASE                      = "gbase";
    String GBASE_DRIVER               = "com.gbase.jdbc.Driver";

    String XUGU                       = "xugu";
    String XUGU_DRIVER                = "com.xugu.cloudjdbc.Driver";

    String OCEANBASE                  = "oceanbase";
    String OCEANBASE_DRIVER           = "com.mysql.jdbc.Driver";
    String INFORMIX                   = "informix";

    /**
     * 阿里云odps
     */
    String ODPS                       = "odps";
    String ODPS_DRIVER                = "com.aliyun.odps.jdbc.OdpsDriver";

    String TERADATA                   = "teradata";
    String TERADATA_DRIVER            = "com.teradata.jdbc.TeraDriver";

    /**
     * Log4JDBC
     */
    String LOG4JDBC                   = "log4jdbc";
    String LOG4JDBC_DRIVER            = "net.sf.log4jdbc.DriverSpy";

    String PHOENIX                    = "phoenix";
    String PHOENIX_DRIVER             = "org.apache.phoenix.jdbc.PhoenixDriver";
    String ENTERPRISEDB               = "edb";
    String ENTERPRISEDB_DRIVER        = "com.edb.Driver";

    String KYLIN                      = "kylin";
    String KYLIN_DRIVER               = "org.apache.kylin.jdbc.Driver";


    String SQLITE                     = "sqlite";
    String SQLITE_DRIVER              = "org.sqlite.JDBC";

    String ALIYUN_ADS                 = "aliyun_ads";
    String ALIYUN_DRDS                = "aliyun_drds";

    String PRESTO                     = "presto";
    String PRESTO_DRIVER              = "com.facebook.presto.jdbc.PrestoDriver";

    String ELASTIC_SEARCH             = "elastic_search";

    String ELASTIC_SEARCH_DRIVER      = "com.alibaba.xdriver.elastic.jdbc.ElasticDriver";

    String CLICKHOUSE                 = "clickhouse";
    String CLICKHOUSE_DRIVER          = "ru.yandex.clickhouse.ClickHouseDriver";

    // for HBase2.X and Phoenix5.X
    String HBASE20XSQL = "hbase20xsql";
    String HBASE20XSQL_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

}
