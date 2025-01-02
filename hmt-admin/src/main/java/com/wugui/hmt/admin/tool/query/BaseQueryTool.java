package com.wugui.hmt.admin.tool.query;

import cn.hutool.core.util.StrUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.wugui.hmt.core.util.Constants;
import com.wugui.hmt.admin.core.util.LocalCacheUtil;
import com.wugui.hmt.admin.entity.JobDatasource;
import com.wugui.hmt.admin.tool.database.ColumnInfo;
import com.wugui.hmt.admin.tool.database.DasColumn;
import com.wugui.hmt.admin.tool.database.TableInfo;
import com.wugui.hmt.admin.tool.meta.DatabaseInterface;
import com.wugui.hmt.admin.tool.meta.DatabaseMetaFactory;
import com.wugui.hmt.admin.tool.pojo.MetaDataPojo;
import com.wugui.hmt.admin.util.*;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;


/*-------------------------------------------------------------------------
 *
 * BaseQueryTool.java
 *  抽象查询工具
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/tool/query/BaseQueryTool.java
 *
 *-----------------------------------------------
 */
public abstract class BaseQueryTool implements QueryToolInterface {

    protected static final Logger logger = LoggerFactory.getLogger(BaseQueryTool.class);
    /**
     * 用于获取查询语句
     */
    private DatabaseInterface sqlBuilder;

    private DataSource datasource;

    private Connection connection;
    /**
     * 当前数据库名
     */
    private String currentSchema;
    private String currentDatabase;

    /**
     * 构造方法
     *
     * @param jobDatasource
     */
    BaseQueryTool(JobDatasource jobDatasource) throws SQLException {
        if (LocalCacheUtil.get(jobDatasource.getDatasourceName()) == null) {
            getDataSource(jobDatasource);
        } else {
            this.connection = (Connection) LocalCacheUtil.get(jobDatasource.getDatasourceName());
            if (!this.connection.isValid(500)) {
                LocalCacheUtil.remove(jobDatasource.getDatasourceName());
                getDataSource(jobDatasource);
            }
        }
        sqlBuilder = DatabaseMetaFactory.getByDbType(jobDatasource.getDatasource());
        currentSchema = getSchema(jobDatasource.getJdbcUsername());
        currentDatabase = jobDatasource.getDatasource();
        LocalCacheUtil.set(jobDatasource.getDatasourceName(), this.connection, 4 * 60 * 60 * 1000);
    }

    private void getDataSource(JobDatasource jobDatasource) throws SQLException {
        String userName = AESUtil.decrypt(jobDatasource.getJdbcUsername());

        //这里默认使用 hikari 数据源
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setUsername(userName);
        dataSource.setPassword(AESUtil.decrypt(jobDatasource.getJdbcPassword()));
        dataSource.setJdbcUrl(jobDatasource.getJdbcUrl());
        dataSource.setDriverClassName(jobDatasource.getJdbcDriverClass());
        dataSource.setMaximumPoolSize(1);
        dataSource.setMinimumIdle(0);
        dataSource.setConnectionTimeout(30000);
        this.datasource = dataSource;
        this.connection = this.datasource.getConnection();
    }

    //根据connection获取schema
    private String getSchema(String jdbcUsername) {
        String res = null;
        try {
            res = connection.getCatalog();
        } catch (SQLException e) {
            try {
                res = connection.getSchema();
            } catch (SQLException e1) {
                logger.error("[SQLException getSchema Exception] --> "
                        + "the exception message is:" + e1.getMessage());
            }
            logger.error("[getSchema Exception] --> "
                    + "the exception message is:" + e.getMessage());
        }
        // 如果res是null，则将用户名当作 schema
        if (StrUtil.isBlank(res) && StringUtils.isNotBlank(jdbcUsername)) {
            res = jdbcUsername.toUpperCase();
        }
        return res;
    }

    @Override
    public TableInfo buildTableInfo(String tableName) {
        //获取表信息
        List<Map<String, Object>> tableInfos = this.getTableInfo(tableName);
        if (tableInfos.isEmpty()) {
            throw new NullPointerException("查询出错! ");
        }

        TableInfo tableInfo = new TableInfo();
        //表名，注释
        List tValues = new ArrayList(tableInfos.get(0).values());

        tableInfo.setName(StrUtil.toString(tValues.get(0)));
        tableInfo.setComment(StrUtil.toString(tValues.get(1)));


        //获取所有字段
        List<ColumnInfo> fullColumn = getColumns(tableName);
        tableInfo.setColumns(fullColumn);

        //获取主键列
        List<String> primaryKeys = getPrimaryKeys(tableName);
        logger.info("主键列为：{}", primaryKeys);

        //设置ifPrimaryKey标志
        fullColumn.forEach(e -> {
            if (primaryKeys.contains(e.getName())) {
                e.setIfPrimaryKey(true);
            } else {
                e.setIfPrimaryKey(false);
            }
        });
        return tableInfo;
    }

    //无论怎么查，返回结果都应该只有表名和表注释，遍历map拿value值即可
    @Override
    public List<Map<String, Object>> getTableInfo(String tableName) {
        String sqlQueryTableNameComment = sqlBuilder.getSQLQueryTableNameComment();
        logger.info(sqlQueryTableNameComment);
        List<Map<String, Object>> res = null;
        try {
            res = JdbcUtils.executeQuery(connection, sqlQueryTableNameComment, ImmutableList.of(currentSchema, tableName));
        } catch (SQLException e) {
            logger.error("[getTableInfo Exception] --> "
                    + "the exception message is:" + e.getMessage());
        }
        return res;
    }

    @Override
    public List<Map<String, Object>> getTables() {
        String sqlQueryTables = sqlBuilder.getSQLQueryTables();
        logger.info(sqlQueryTables);
        List<Map<String, Object>> res = null;
        try {
            res = JdbcUtils.executeQuery(connection, sqlQueryTables, ImmutableList.of(currentSchema));
        } catch (SQLException e) {
            logger.error("[getTables Exception] --> "
                    + "the exception message is:" + e.getMessage());
        }
        return res;
    }

    @Override
    public List<ColumnInfo> getColumns(String tableName) {

        List<ColumnInfo> fullColumn = Lists.newArrayList();
        //获取指定表的所有字段
        try {
            //获取查询指定表所有字段的sql语句
            String querySql = sqlBuilder.getSQLQueryFields(tableName);
            logger.info("querySql: {}", querySql);

            //获取所有字段
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(querySql);
            ResultSetMetaData metaData = resultSet.getMetaData();

            List<DasColumn> dasColumns = buildDasColumn(tableName, metaData);
            statement.close();

            //构建 fullColumn
            fullColumn = buildFullColumn(dasColumns);

        } catch (SQLException e) {
            logger.error("[getColumns Exception] --> "
                    + "the exception message is:" + e.getMessage());
        }
        return fullColumn;
    }

    private List<ColumnInfo> buildFullColumn(List<DasColumn> dasColumns) {
        List<ColumnInfo> res = Lists.newArrayList();
        dasColumns.forEach(e -> {
            ColumnInfo columnInfo = new ColumnInfo();
            columnInfo.setName(e.getColumnName());
            columnInfo.setComment(e.getColumnComment());
            columnInfo.setType(e.getColumnTypeName());
            columnInfo.setIfPrimaryKey(e.isIsprimaryKey());
            columnInfo.setIsnull(e.getIsNull());
            res.add(columnInfo);
        });
        return res;
    }

    //构建DasColumn对象
    private List<DasColumn> buildDasColumn(String tableName, ResultSetMetaData metaData) {
        List<DasColumn> res = Lists.newArrayList();
        try {
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                DasColumn dasColumn = new DasColumn();
                dasColumn.setColumnClassName(metaData.getColumnClassName(i));
                dasColumn.setColumnTypeName(metaData.getColumnTypeName(i));
                dasColumn.setColumnName(metaData.getColumnName(i));
                dasColumn.setIsNull(metaData.isNullable(i));

                res.add(dasColumn);
            }

            Statement statement = connection.createStatement();

            if (currentDatabase.equals(JdbcConstants.MYSQL) || currentDatabase.equals(JdbcConstants.ORACLE)) {
                DatabaseMetaData databaseMetaData = connection.getMetaData();

                ResultSet resultSet = databaseMetaData.getPrimaryKeys(null, null, tableName);

                while (resultSet.next()) {
                    String name = resultSet.getString("COLUMN_NAME");
                    res.forEach(e -> {
                        if (e.getColumnName().equals(name)) {
                            e.setIsprimaryKey(true);

                        } else {
                            e.setIsprimaryKey(false);
                        }
                    });
                }

                res.forEach(e -> {
                    String sqlQueryComment = sqlBuilder.getSQLQueryComment(currentSchema, tableName, e.getColumnName());
                    //查询字段注释
                    try {
                        ResultSet resultSetComment = statement.executeQuery(sqlQueryComment);
                        while (resultSetComment.next()) {
                            e.setColumnComment(resultSetComment.getString(1));
                        }
                        JdbcUtils.close(resultSetComment);
                    } catch (SQLException e1) {
                        logger.error("[buildDasColumn executeQuery Exception] --> "
                                + "the exception message is:" + e1.getMessage());
                    }
                });
            }

            JdbcUtils.close(statement);
        } catch (SQLException e) {
            logger.error("[buildDasColumn Exception] --> "
                    + "the exception message is:" + e.getMessage());
        }
        return res;
    }

    //获取指定表的主键，可能是多个，所以用list
    private List<String> getPrimaryKeys(String tableName) {
        List<String> res = Lists.newArrayList();
        String sqlQueryPrimaryKey = sqlBuilder.getSQLQueryPrimaryKey();
        try {
            List<Map<String, Object>> pkColumns = JdbcUtils.executeQuery(connection, sqlQueryPrimaryKey, ImmutableList.of(currentSchema, tableName));
            //返回主键名称即可
            pkColumns.forEach(e -> res.add((String) new ArrayList<>(e.values()).get(0)));
        } catch (SQLException e) {
            logger.error("[getPrimaryKeys Exception] --> "
                    + "the exception message is:" + e.getMessage());
        }
        return res;
    }

    @Override
    public List<String> getColumnNames(String tableSchema, String tableName, String datasource) {

        List<String> res = Lists.newArrayList();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            if(JdbcConstants.HALO.equals(datasource) || JdbcConstants.ORACLE.equals(datasource) || JdbcConstants.DM.equals(datasource) || JdbcConstants.DB2.equals(datasource) || JdbcConstants.MYSQL.equals(datasource)){
                // 先将schema去除
                tableName = tableName.replaceAll(tableSchema + "\\.","");
                // 特殊字符或中文加上双引号
                if(JdbcConstants.HALO.equals(datasource)) {
                    if (SubMetaUtil.colNameIsNormal(tableName)) {
                        tableName = String.format("\"%s\"", tableName);
                    }
                } else if(JdbcConstants.ORACLE.equals(datasource)) {
                    if (SubMetaUtil.oraColNameIsNormal(tableName)) {
                        tableName = String.format("\"%s\"", tableName);
                    }
                } else if(JdbcConstants.DM.equals(datasource)) {
                    if (SubMetaUtil.oraColNameIsNormal(tableName)) {
                        tableName = String.format("\"%s\"", tableName);
                    }
                } else if(JdbcConstants.MYSQL.equals(datasource)) {
                    tableName = String.format("`%s`", tableName);
                } else{
                    if (SubMetaUtil.oraColNameIsNormal(tableName)) {
                        tableName = String.format("\"%s\"", tableName);
                    }
                }
                tableName = tableSchema + "." + tableName;
            }
            //获取查询指定表所有字段的sql语句
            String querySql = sqlBuilder.getSQLQueryFields(tableName);
            logger.info("querySql: {}", querySql);

            //获取所有字段
            stmt = connection.createStatement();
            rs = stmt.executeQuery(querySql);
            ResultSetMetaData metaData = rs.getMetaData();

            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                if (JdbcConstants.HIVE.equals(datasource)) {
                    if (columnName.contains(Constants.SPLIT_POINT)) {
                        res.add(i - 1 + Constants.SPLIT_SCOLON + columnName.substring(columnName.indexOf(Constants.SPLIT_POINT) + 1) + Constants.SPLIT_SCOLON + metaData.getColumnTypeName(i));
                    } else {
                        res.add(i - 1 + Constants.SPLIT_SCOLON + columnName + Constants.SPLIT_SCOLON + metaData.getColumnTypeName(i));
                    }
                } else {
                    res.add(columnName);
                }

            }
        } catch (SQLException e) {
            logger.error("[getColumnNames Exception] --> "
                    + "the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return res;
    }

    @Override
    public List<String> getTableNames(String tableSchema) {
        List<String> tables = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = getSQLQueryTables(tableSchema);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String tableName = rs.getString(1);
                tables.add(tableName);
            }
            tables.sort(Comparator.naturalOrder());
        } catch (SQLException e) {
            logger.error("[getTableNames Exception] --> "
                    + "the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return tables;
    }

    @Override
    public List<String> getTableNames() {
        List<String> tables = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = getSQLQueryTables();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String tableName = rs.getString(1);
                tables.add(tableName);
            }
        } catch (SQLException e) {
            logger.error("[getTableNames Exception] --> "
                    + "the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return tables;
    }

    public Boolean dataSourceTest() {
        try {
            DatabaseMetaData metaData = connection.getMetaData();
            if (metaData.getDatabaseProductName().length() > 0) {
                return true;
            }
        } catch (SQLException e) {
            logger.error("[dataSourceTest Exception] --> "
                    + "the exception message is:" + e.getMessage());
        }
        return false;
    }


    protected String getSQLQueryTables(String tableSchema) {
        return sqlBuilder.getSQLQueryTables(tableSchema);
    }

    /**
     * 不需要其他参数的可不重写
     *
     * @return
     */
    protected String getSQLQueryTables() {
        return sqlBuilder.getSQLQueryTables();
    }

    @Override
    public List<String> getColumnsByQuerySql(String querySql) throws SQLException {

        List<String> res = Lists.newArrayList();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            querySql = querySql.replace(";", "");
            //拼装sql语句，在后面加上 where 1=0 即可
            String sql = querySql.concat(" where 1=0");
            //判断是否已有where，如果是，则加 and 1=0
            //从最后一个 ) 开始找 where，或者整个语句找
            if (querySql.contains(")")) {
                if (querySql.substring(querySql.indexOf(")")).contains("where")) {
                    sql = querySql.concat(" and 1=0");
                }
            } else {
                if (querySql.contains("where")) {
                    sql = querySql.concat(" and 1=0");
                }
            }
            //获取所有字段
            stmt = connection.createStatement();
            rs = stmt.executeQuery(sql);
            ResultSetMetaData metaData = rs.getMetaData();

            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                res.add(metaData.getColumnName(i));
            }
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return res;
    }

    @Override
    public long getMaxIdVal(String tableName, String primaryKey) {
        Statement stmt = null;
        ResultSet rs = null;
        long maxVal = 0;
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = getSQLMaxID(tableName, primaryKey);
            rs = stmt.executeQuery(sql);
            rs.next();
            maxVal = rs.getLong(1);
        } catch (SQLException e) {
            logger.error("[getMaxIdVal Exception] --> "
                    + "the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }


        return maxVal;
    }

    private String getSQLMaxID(String tableName, String primaryKey) {
        return sqlBuilder.getMaxId(tableName, primaryKey);
    }

    @Override
    public List<String> getNoPrimaryKeyTableNames(String tableSchema) {
        List<String> tables = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = getSQLQueryNoPrimaryKeyTables(tableSchema);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String tableName = rs.getString(1);
                tables.add(tableName);
            }
            tables.sort(Comparator.naturalOrder());
        } catch (SQLException e) {
            logger.error("[getTableNames Exception] --> "
                    + "the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return tables;
    }

    protected String getSQLQueryNoPrimaryKeyTables(String tableSchema) {
        return sqlBuilder.getSQLQueryNoPrimaryKeyTables(tableSchema);
    }

    @Override
    public List<String> getTableNamesNoSchema(String tableSchema) {
        List<String> tables = new ArrayList<String>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = getSQLQueryTablesNoSchema(tableSchema);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String tableName = rs.getString(1);
                tables.add(tableName);
            }
            tables.sort(Comparator.naturalOrder());
        } catch (SQLException e) {
            logger.error("[getTableNames Exception] --> "
                    + "the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return tables;
    }

    protected String getSQLQueryTablesNoSchema(String tableSchema) {
        return sqlBuilder.getSQLQueryTablesNoSchema(tableSchema);
    }

    @Override
    public Long getTableRows(String tableName) {
        Statement stmt = null;
        ResultSet rs = null;
        Long rows = 0L;
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = getSQLQueryTableRows(tableName);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                rows = rs.getLong(1);
            }
        } catch (SQLException e) {
            logger.error("[getTableRows Exception] --> "
                    + "the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return rows;
    }

    protected String getSQLQueryTableRows(String tableName) {
        return sqlBuilder.getTableRows(tableName);
    }

    @Override
    public List<Map<String, String>> getTableColumnsAndType(String tableSchema, String tableName) {
        Statement stmt = null;
        ResultSet rs = null;
        List<Map<String, String>> mapList = new ArrayList<>();
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = getSQLQueryTableColumnsAndType(tableSchema, tableName);
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                Map<String, String> map = new HashMap<>();
                map.put("COLUMN_NAME", rs.getString(1));
                map.put("DATA_TYPE", rs.getString(2));
                map.put("DATA_LENGTH", rs.getString(3));
                map.put("IS_NULLABLE", rs.getString(4));
                map.put("COLUMN_DEFAULT", rs.getString(5));
                mapList.add(map);
            }
        } catch (SQLException e) {
            logger.error("[getTableColumnsAndType Exception] --> "
                    + "the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return mapList;
        // Statement stmt = null;
        // ResultSet rs = null;
        // List<Map<String, String>> mapList = new ArrayList<>();
        // try {
        //     stmt = connection.createStatement();
        //     rs = stmt.executeQuery(getSQLQueryTableColumnsAndType(tableSchema, tableName));
        //     // 获取结果集结构（元数据）
        //     ResultSetMetaData rmd = rs.getMetaData();
        //     // 获取字段数（即每条记录有多少个字段）
        //     int columnCount = rmd.getColumnCount();
        //     while (rs.next()) {
        //         // 保存记录中的每个<字段名-字段值>
        //         Map<String, String> rowData = new HashMap<String, String>();
        //         for (int i = 1; i <= columnCount; ++i) {
        //             // <字段名-字段值>
        //             rowData.put(rmd.getColumnName(i), rs.getString(i));
        //         }
        //         mapList.add(rowData);
        //     }
        // } catch (SQLException e) {
        //     logger.error("[getTableColumnsAndType Exception] --> "
        //             + "the exception message is:" + e.getMessage());
        //     // 此处借用此异常抛出
        //     throw new UnsupportedOperationException(e.getMessage());
        // } finally {
        //     JdbcUtils.close(rs);
        //     JdbcUtils.close(stmt);
        // }
        // return mapList;
    }

    protected String getSQLQueryTableColumnsAndType(String tableSchema, String tableName) {
        return sqlBuilder.getTableColumnsAndType(tableSchema, tableName);
    }

    public void executeCreateTableSql(String querySql) {
        if (StringUtils.isBlank(querySql)) {
            return;
        }
        Statement stmt = null;
        try {
            stmt = connection.createStatement();
            stmt.executeUpdate(querySql);
        } catch (SQLException e) {
            logger.error("[executeCreateTableSql Exception] --> "
                    + "the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(stmt);
        }
    }

    public List<String> getTableSchema() {
        List<String> schemas = new ArrayList<>();
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = getSQLQueryTableSchema();
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String tableName = rs.getString(1);
                schemas.add(tableName);
            }
        } catch (SQLException e) {
            logger.error("[getTableNames Exception] --> "
                    + "the exception message is:" + e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return schemas;
    }

    protected String getSQLQueryTableSchema() {
        return sqlBuilder.getSQLQueryTableSchema();
    }


    public int getMetaDataCountSQLs(String executeSql) {
        Statement stmt = null;
        ResultSet rs = null;
        int count = 0 ;
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = executeSql;
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                count = rs.getInt(1);
            }
        } catch (SQLException e) {
            logger.error("[getMetaDataSQLs Exception] --> "
                    + "the exception message is:" + e.getMessage());
            // 此处借用此异常抛出
            throw new UnsupportedOperationException(e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return count;
    }

    public List<MetaDataPojo> getMetaDataSQLs(String executeSql) {
        Statement stmt = null;
        ResultSet rs = null;
        List<MetaDataPojo> metaDataPojoList = new ArrayList<>();
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = executeSql;
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String sqlInfo = rs.getString(1);
                String objectName = rs.getString(2);
                MetaDataPojo metaDataPojo = new MetaDataPojo();
                metaDataPojo.setSqlInfo(sqlInfo);
                metaDataPojo.setObjectName(objectName);
                metaDataPojoList.add(metaDataPojo);
            }
        } catch (SQLException e) {
            logger.error("[getMetaDataSQLs Exception] --> "
                    + "the exception message is:" + e.getMessage());
            // 此处借用此异常抛出
            throw new UnsupportedOperationException(e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return metaDataPojoList;
    }

    public List<MetaDataPojo> getMetaDataSQLs(String executeSql, String beforeSql) {
        Statement stmt = null;
        ResultSet rs = null;
        List<MetaDataPojo> metaDataPojoList = new ArrayList<>();
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = beforeSql;
            rs = stmt.executeQuery(sql);
            //获取sql
            sql = executeSql;
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                MetaDataPojo metaDataPojo = new MetaDataPojo();
                metaDataPojo.setSqlInfo(rs.getString(1));
                metaDataPojo.setObjectName(rs.getString(2));
                metaDataPojo.setPartitioningType(rs.getString(3));
                metaDataPojo.setSubPartitioningType(rs.getString(4));
                metaDataPojo.setTemporary(rs.getString(5));
                metaDataPojoList.add(metaDataPojo);
            }
        } catch (SQLException e) {
            logger.error("[getMetaDataSQLs Exception] --> "
                    + "the exception message is:" + e.getMessage());
            // 此处借用此异常抛出
            throw new UnsupportedOperationException(e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return metaDataPojoList;
    }

    public List<Map<String, String>> getMetaPublicSQLs(String executeSql, int type) {
        Statement stmt = null;
        ResultSet rs = null;
        List<Map<String, String>> metaDataPojoList = new ArrayList<>();
        try {
            stmt = connection.createStatement();
            //获取sql
            String sql = executeSql;
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                Map<String, String> map = new HashMap<>();
                if(type == SubMetaEnum.CONSTRAINT_TYPE_1.getTypeId()) {
                    map.put("CONSTRAINT_NAME", rs.getString("CONSTRAINT_NAME"));
                    map.put("SEARCH_CONDITION", rs.getString("SEARCH_CONDITION"));
                    map.put("TABLE_NAME", rs.getString("TABLE_NAME"));
                    map.put("OWNER", rs.getString("OWNER"));
                    map.put("TEMPORARY", rs.getString("TEMPORARY"));
                } else if(type == SubMetaEnum.CONSTRAINT_TYPE_2.getTypeId()){
                    map.put("OWNER", rs.getString("OWNER"));
                    map.put("TABLE_NAME", rs.getString("TABLE_NAME"));
                    map.put("CONSTRAINT_NAME", rs.getString("CONSTRAINT_NAME"));
                    map.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));
                    map.put("TEMPORARY", rs.getString("TEMPORARY"));
                } else if(type == SubMetaEnum.CONSTRAINT_TYPE_3.getTypeId()){
                    map.put("CONSTRAINT_NAME", rs.getString("CONSTRAINT_NAME"));
                    map.put("OWNER", rs.getString("OWNER"));
                    map.put("TABLE_NAME", rs.getString("TABLE_NAME"));
                    map.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));
                    map.put("R_TABLE_NAME", rs.getString("R_TABLE_NAME"));
                    map.put("R_CONSTRAINT_NAME", rs.getString("R_CONSTRAINT_NAME"));
                    map.put("R_COLUMN_NAME", rs.getString("R_COLUMN_NAME"));
                    map.put("R_OWNER", rs.getString("R_OWNER"));
                    map.put("DELETE_RULE", rs.getString("DELETE_RULE"));
                    map.put("DEFERRED", rs.getString("DEFERRED"));
                    map.put("POSITION", rs.getString("POSITION"));
                    map.put("VALIDATED", rs.getString("VALIDATED"));
                } else if(type == SubMetaEnum.TABLE_COMMENTS_1.getTypeId()){
                    map.put("TABLE_NAME", rs.getString("TABLE_NAME"));
                    map.put("COMMENTS", rs.getString("COMMENTS"));
                } else if(type == SubMetaEnum.TABLE_COMMENTS_2.getTypeId()){
                    map.put("TABLE_NAME", rs.getString("TABLE_NAME"));
                    map.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));
                    map.put("COMMENTS", rs.getString("COMMENTS"));
                } else if(type == SubMetaEnum.TRIGGER_TYPE_1.getTypeId()){
                    map.put("TRIGGER_NAME", rs.getString("TRIGGER_NAME"));
                    map.put("TRIGGER_TYPE", rs.getString("TRIGGER_TYPE"));
                    map.put("TRIGGERING_EVENT", rs.getString("TRIGGERING_EVENT"));
                    map.put("TABLE_NAME", rs.getString("TABLE_NAME"));
                    map.put("TRIGGER_BODY", rs.getString("TRIGGER_BODY"));
                    map.put("WHEN_CLAUSE", rs.getString("WHEN_CLAUSE"));
                    map.put("DESCRIPTION", rs.getString("DESCRIPTION"));
                    map.put("ACTION_TYPE", rs.getString("ACTION_TYPE"));
                    map.put("OWNER", rs.getString("OWNER"));
                } else if(type == SubMetaEnum.PART_TABLE_1.getTypeId()){
                    map.put("HIGH_VALUE", rs.getString("HIGH_VALUE"));
                    map.put("PARTITION_NAME", rs.getString("PARTITION_NAME"));
                    map.put("TABLE_NAME", rs.getString("TABLE_NAME"));
                    map.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));
                    map.put("PARTITIONING_TYPE", rs.getString("PARTITIONING_TYPE"));
                } else if(type == SubMetaEnum.PART_TABLE_2.getTypeId()){
                    map.put("HIGH_VALUE", rs.getString("HIGH_VALUE"));
                    map.put("SUBPARTITION_NAME", rs.getString("SUBPARTITION_NAME"));
                    map.put("SUBPARTITION_POSITION", rs.getString("SUBPARTITION_POSITION"));
                    map.put("PARTITION_NAME", rs.getString("PARTITION_NAME"));
                    map.put("TABLE_NAME", rs.getString("TABLE_NAME"));
                    map.put("COLUMN_NAME", rs.getString("COLUMN_NAME"));
                    map.put("SUBPARTITIONING_TYPE", rs.getString("SUBPARTITIONING_TYPE"));
                } else if(type == SubMetaEnum.TABLE_NAME_1.getTypeId()){
                    map.put("TABLE_NAME", rs.getString("TABLE_NAME"));
                }
                metaDataPojoList.add(map);
            }
        } catch (SQLException e) {
            logger.error("[getMetaConstraintsSQLs Exception] --> "
                    + "the exception message is:" + e.getMessage());
            // 此处借用此异常抛出
            throw new UnsupportedOperationException(e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return metaDataPojoList;
    }

    public List<Map<String, String>> getMetaDataUnificationSql(String sql) {
        Statement stmt = null;
        ResultSet rs = null;
        List<Map<String, String>> metaDataPojoList = new ArrayList<>();
        try {
            stmt = connection.createStatement();
            rs = stmt.executeQuery(sql);
            // 获取结果集结构（元数据）
            ResultSetMetaData rmd = rs.getMetaData();
            // 获取字段数（即每条记录有多少个字段）
            int columnCount = rmd.getColumnCount();
            while (rs.next()) {
                // 保存记录中的每个<字段名-字段值>
                Map<String, String> rowData = new HashMap<String, String>();
                for (int i = 1; i <= columnCount; ++i) {
                    // <字段名-字段值>
                    rowData.put(rmd.getColumnName(i), rs.getString(i));
                }
                metaDataPojoList.add(rowData);
            }
        } catch (SQLException e) {
            logger.error("[getMetaDataUnificationSql Exception] --> "
                    + "the exception message is:" + e.getMessage());
            // 此处借用此异常抛出
            throw new UnsupportedOperationException(e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return metaDataPojoList;
    }

    public boolean checkSchemaSql(String schema) {
        Statement stmt = null;
        ResultSet rs = null;
        boolean schemaExists = false;
        try {
            stmt = connection.createStatement();
            String sql = "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = '"+schema+"')";
            rs = stmt.executeQuery(sql);
            while (rs.next()) {
                schemaExists = rs.getBoolean(1);
            }
        } catch (SQLException e) {
            logger.error("[checkSchemaSql Exception] --> "
                    + "the exception message is:" + e.getMessage());
            // 此处借用此异常抛出
            throw new UnsupportedOperationException(e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return schemaExists;
    }

    public Boolean createSchemaSQL(String schema) {
        Statement stmt = null;
        ResultSet rs = null;
        boolean result = false;
        try {
            stmt = connection.createStatement();
            String sql = "CREATE SCHEMA "+schema+";";
            stmt.executeUpdate(sql);
            result = true;
        } catch (SQLException e) {
            logger.error("[checkSchemaSql Exception] --> "
                    + "the exception message is:" + e.getMessage());
            // 此处借用此异常抛出
            throw new UnsupportedOperationException(e.getMessage());
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(stmt);
        }
        return result;
    }

}
