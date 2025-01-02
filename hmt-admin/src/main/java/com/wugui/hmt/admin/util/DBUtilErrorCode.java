package com.wugui.hmt.admin.util;


/*-------------------------------------------------------------------------
 *
 * DBUtilErrorCode.java
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/util/DBUtilErrorCode.java
 *
 *-----------------------------------------------
 */
public enum DBUtilErrorCode implements ErrorCode {
    //连接错误
    MYSQL_CONN_USERPWD_ERROR("MYSQLErrCode-01","数据库用户名或者密码错误，请检查填写的账号密码或者联系DBA确认账号和密码是否正确"),
    MYSQL_CONN_IPPORT_ERROR("MYSQLErrCode-02","数据库服务的IP地址或者Port错误，请检查填写的IP地址和Port或者联系DBA确认IP地址和Port是否正确。如果是同步中心用户请联系DBA确认idb上录入的IP和PORT信息和数据库的当前实际信息是一致的"),
    MYSQL_CONN_DB_ERROR("MYSQLErrCode-03","数据库名称错误，请检查数据库实例名称或者联系DBA确认该实例是否存在并且在正常服务"),

    ORACLE_CONN_USERPWD_ERROR("ORACLEErrCode-01","数据库用户名或者密码错误，请检查填写的账号密码或者联系DBA确认账号和密码是否正确"),
    ORACLE_CONN_IPPORT_ERROR("ORACLEErrCode-02","数据库服务的IP地址或者Port错误，请检查填写的IP地址和Port或者联系DBA确认IP地址和Port是否正确。如果是同步中心用户请联系DBA确认idb上录入的IP和PORT信息和数据库的当前实际信息是一致的"),
    ORACLE_CONN_DB_ERROR("ORACLEErrCode-03","数据库名称错误，请检查数据库实例名称或者联系DBA确认该实例是否存在并且在正常服务"),

    //execute query错误
    MYSQL_QUERY_TABLE_NAME_ERROR("MYSQLErrCode-04","表不存在，请检查表名或者联系DBA确认该表是否存在"),
    MYSQL_QUERY_SQL_ERROR("MYSQLErrCode-05","SQL语句执行出错，请检查Where条件是否存在拼写或语法错误"),
    MYSQL_QUERY_COLUMN_ERROR("MYSQLErrCode-06","Column信息错误，请检查该列是否存在，如果是常量或者变量，请使用英文单引号’包起来"),
    MYSQL_QUERY_SELECT_PRI_ERROR("MYSQLErrCode-07","读表数据出错，因为账号没有读表的权限，请联系DBA确认该账号的权限并授权"),

    ORACLE_QUERY_TABLE_NAME_ERROR("ORACLEErrCode-04","表不存在，请检查表名或者联系DBA确认该表是否存在"),
    ORACLE_QUERY_SQL_ERROR("ORACLEErrCode-05","SQL语句执行出错，原因可能是你填写的列不存在或者where条件不符合要求，1，请检查该列是否存在，如果是常量或者变量，请使用英文单引号’包起来;  2，请检查Where条件是否存在拼写或语法错误"),
    ORACLE_QUERY_SELECT_PRI_ERROR("ORACLEErrCode-06","读表数据出错，因为账号没有读表的权限，请联系DBA确认该账号的权限并授权"),
    ORACLE_QUERY_SQL_PARSER_ERROR("ORACLEErrCode-07","SQL语法出错，请检查Where条件是否存在拼写或语法错误"),

    //PreSql,PostSql错误
    MYSQL_PRE_SQL_ERROR("MYSQLErrCode-08","PreSQL语法错误，请检查"),
    MYSQL_POST_SQL_ERROR("MYSQLErrCode-09","PostSql语法错误，请检查"),
    MYSQL_QUERY_SQL_PARSER_ERROR("MYSQLErrCode-10","SQL语法出错，请检查Where条件是否存在拼写或语法错误"),

    ORACLE_PRE_SQL_ERROR("ORACLEErrCode-08", "PreSQL语法错误，请检查"),
    ORACLE_POST_SQL_ERROR("ORACLEErrCode-09", "PostSql语法错误，请检查"),

    //SplitPK 错误
    MYSQL_SPLIT_PK_ERROR("MYSQLErrCode-11","SplitPK错误，请检查"),
    ORACLE_SPLIT_PK_ERROR("ORACLEErrCode-10","SplitPK错误，请检查"),

    //Insert,Delete 权限错误
    MYSQL_INSERT_ERROR("MYSQLErrCode-12","数据库没有写权限，请联系DBA"),
    MYSQL_DELETE_ERROR("MYSQLErrCode-13","数据库没有Delete权限，请联系DBA"),
    ORACLE_INSERT_ERROR("ORACLEErrCode-11","数据库没有写权限，请联系DBA"),
    ORACLE_DELETE_ERROR("ORACLEErrCode-12","数据库没有Delete权限，请联系DBA"),

    JDBC_NULL("DBUtilErrorCode-20","JDBC URL为空，请检查配置"),
    JDBC_OB10_ADDRESS_ERROR("DBUtilErrorCode-OB10-01","JDBC OB10格式错误，请联系ask hmt"),
    CONF_ERROR("DBUtilErrorCode-00", "您的配置错误."),
    CONN_DB_ERROR("DBUtilErrorCode-10", "连接数据库失败. 请检查您的 账号、密码、数据库名称、IP、Port或者向 DBA 寻求帮助(注意网络环境)."),
    GET_COLUMN_INFO_FAILED("DBUtilErrorCode-01", "获取表字段相关信息失败."),
    UNSUPPORTED_TYPE("DBUtilErrorCode-12", "不支持的数据库类型. 请注意查看 Hmt 已经支持的数据库类型以及数据库版本."),
    COLUMN_SPLIT_ERROR("DBUtilErrorCode-13", "根据主键进行切分失败."),
    SET_SESSION_ERROR("DBUtilErrorCode-14", "设置 session 失败."),
    RS_ASYNC_ERROR("DBUtilErrorCode-15", "异步获取ResultSet next失败."),

    REQUIRED_VALUE("DBUtilErrorCode-03", "您缺失了必须填写的参数值."),
    ILLEGAL_VALUE("DBUtilErrorCode-02", "您填写的参数值不合法."),
    ILLEGAL_SPLIT_PK("DBUtilErrorCode-04", "您填写的主键列不合法, Hmt 仅支持切分主键为一个,并且类型为整数或者字符串类型."),
    SPLIT_FAILED_ILLEGAL_SQL("DBUtilErrorCode-15", "Hmt尝试切分表时, 执行数据库 Sql 失败. 请检查您的配置 table/splitPk/where 并作出修改."),
    SQL_EXECUTE_FAIL("DBUtilErrorCode-06", "执行数据库 Sql 失败, 请检查您的配置的 column/table/where/querySql或者向 DBA 寻求帮助."),

    // only for reader
    READ_RECORD_FAIL("DBUtilErrorCode-07", "读取数据库数据失败. 请检查您的配置的 column/table/where/querySql或者向 DBA 寻求帮助."),
    TABLE_QUERYSQL_MIXED("DBUtilErrorCode-08", "您配置凌乱了. 不能同时既配置table又配置querySql"),
    TABLE_QUERYSQL_MISSING("DBUtilErrorCode-09", "您配置错误. table和querySql 应该并且只能配置一个."),

    // only for writer
    WRITE_DATA_ERROR("DBUtilErrorCode-05", "往您配置的写入表中写入数据时失败."),
    NO_INSERT_PRIVILEGE("DBUtilErrorCode-11", "数据库没有写权限，请联系DBA"),
    NO_DELETE_PRIVILEGE("DBUtilErrorCode-16", "数据库没有DELETE权限，请联系DBA"),
    ;

    private final String code;

    private final String description;

    private DBUtilErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }
}
