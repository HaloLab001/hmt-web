package com.wugui.hmt.admin.util;

import cn.hutool.core.lang.Snowflake;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ibm.icu.impl.data.ResourceReader;
import com.wugui.hmt.admin.core.conf.JobAdminConfig;
import com.wugui.hmt.admin.entity.JobDataTypeMapping;
import com.wugui.hmt.admin.entity.JobDatasource;
import com.wugui.hmt.admin.entity.JobInfo;
import com.wugui.hmt.admin.tool.pojo.CreateMetaPojo;
import com.wugui.hmt.admin.tool.pojo.MetaTypePojo;
import com.wugui.hmt.admin.tool.pojo.SubTemplatePojo;

import org.springframework.beans.BeanUtils;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.logging.log4j.util.Strings.isBlank;


/*-------------------------------------------------------------------------
 *
 * SubMetaUtil.java
 *  元数据工具类
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/util/SubMetaUtil.java
 *
 *-----------------------------------------------
 */
public class SubMetaUtil {

    /**
     * 构建shell起始文本模板
     * @return
     */
    public static Map<String, String> buildText(CreateMetaPojo createMetaInfo, int type, JobDatasource datasource, String readerDatasource) {
        String jdbcUrl = datasource.getJdbcUrl();
        String host = cutString(jdbcUrl, "jdbc:halo://",":");
        String port = cutString(jdbcUrl, "jdbc:halo://"+host+":","/");
        String user = AESUtil.decrypt(datasource.getJdbcUsername());
        String pwd = AESUtil.decrypt(datasource.getJdbcPassword());
        String dbName = cutString(jdbcUrl, "jdbc:halo://"+host+":"+port+"/","");
        Map<String, String> map = new HashMap<>();
        String beginTest = "";
        String endTest = "";
        String publicBeginTest = "#！/bin/bash\n" +
                "export PGPASSWORD=\""+pwd+"\"\n" +
                "psql -h "+host+"  -p "+port+" -U "+user+" "+dbName+" -v ON_ERROR_STOP=on";
        // type1:不加事务 type2:加事务 type3:针对表结构执行 type4:针对Package执行
        if(type == SubMetaEnum.BUILD_TEXT_1.getTypeId()) {
            //开始字符
            beginTest = publicBeginTest +" -c \"\nset search_path to "+createMetaInfo.getSchema()+","+readerDatasource+",public; \n";
            //结束字符
            endTest = "\n\"";
        } else if (type == SubMetaEnum.BUILD_TEXT_2.getTypeId()){
            //开始字符
            beginTest = publicBeginTest +" -c '\nBEGIN;\n";
            //结束字符
            endTest = "\nCOMMIT;\n'";
        } else if(type == SubMetaEnum.BUILD_TEXT_3.getTypeId()){
            //建表语句需要将成功的先建进去,失败的进行反馈,所以此处去掉参数 ON_ERROR_STOP
            publicBeginTest = publicBeginTest.replaceAll("-v ON_ERROR_STOP=on", "");
            //开始字符
            beginTest = publicBeginTest +" -f ";
            //结束字符
            endTest = "";
        } else if(type == SubMetaEnum.BUILD_TEXT_4.getTypeId()){
            //hsql
            publicBeginTest = publicBeginTest.replaceAll("psql", "hsql");
            //开始字符
            beginTest = publicBeginTest +" -f ";
            //结束字符
            endTest = "";
        } else if(type == SubMetaEnum.BUILD_TEXT_5.getTypeId()){
            //开始字符
            beginTest = publicBeginTest +" -f ";
            //结束字符
            endTest = "";
        }
        map.put("beginTest", beginTest);
        map.put("endTest", endTest);
        return map;
    }

    /**
     * 构建shell起始文本模板(MySQL命令)
     * @return
     */
    public static Map<String, String> buildMySQLText(CreateMetaPojo createMetaInfo, String type) {
        Map<String, String> map = new HashMap<>();
        String ip = (!createMetaInfo.getMysqlIp().equals("")) ? createMetaInfo.getMysqlIp() : "127.0.0.1";
        String port = (!createMetaInfo.getMysqlPort().equals("")) ? createMetaInfo.getMysqlPort() : "3307";
        String user = (!createMetaInfo.getMysqlUser().equals("")) ? createMetaInfo.getMysqlUser() : "root";
        String pwd = (!createMetaInfo.getMysqlPwd().equals("")) ? createMetaInfo.getMysqlPwd() : "123456";
        if(type.equals("-c")){
            map.put("beginTest", "#！/bin/bash\n" + 
                         "export MYSQL_PWD=\""+pwd+"\"\n" +
                         "mysql -h "+ip+" -D "+createMetaInfo.getSchema()+" -P "+port+" -u "+user+" -e \"");
            map.put("endTest", "\"");
        } else{
            map.put("beginTest", "#！/bin/bash\n" + 
                         "export MYSQL_PWD=\""+pwd+"\"\n" +
                         "mysql -h "+ip+" -D "+createMetaInfo.getSchema()+" -P "+port+" -u "+user+" -f < ");
            map.put("endTest", "");
        }
        return map;
    }


    /**
     * 构建shell任务模板
     * @param createMetaInfo
     * @return
     */
    public static JobInfo buildTemplateInfo(CreateMetaPojo createMetaInfo, SubTemplatePojo subTemplatePojo) {
        //任务构建
        JobInfo jobInfo = new JobInfo();
        BeanUtils.copyProperties(subTemplatePojo, jobInfo);
        jobInfo.setUserId(createMetaInfo.getUserId());
        // add in db
        jobInfo.setGlueType("GLUE_SHELL");
        jobInfo.setExecutorHandler("");
        jobInfo.setReplaceParamType("Timestamp");
        return jobInfo;
    }

    /**
     * 生成shell文本
     * @param startPointMap
     * @param sqlInfo
     * @return
     */
    public static String createShell(Map<String, String> startPointMap, String sqlInfo) {
        //最终脚本
        String shellText = "";
        //拼接
        shellText = startPointMap.get("beginTest") + sqlInfo + startPointMap.get("endTest");
        return shellText;
    }

    /**
     * 字符串截取中间部分
     */
    public static String cutString(String str, String start, String end) {
        try {
            if (isBlank(str)) {
                return str;
            }
            String reg = start + "(.*)" + end;
            Pattern pattern = Pattern.compile(reg);
            Matcher matcher = pattern.matcher(str);
            while (matcher.find()) {
                str = matcher.group(1);
            }
        } catch (Exception e) {

        }
        return str;
    }

    /**
     * 获取元数据类型集合
     * @return
     */
    public static List<MetaTypePojo> getMetaTypePojoList(){
        List<MetaTypePojo> metaDashboardPojoList = new ArrayList<>();
        for (SubMetaTypeEnum subMetaEnum : EnumSet.allOf(SubMetaTypeEnum.class)) {
            MetaTypePojo metaTypePojo = new MetaTypePojo();
            metaTypePojo.setMetaTypeId(subMetaEnum.getMetaTypeId());
            metaTypePojo.setMetaTypeName(subMetaEnum.getMetaTypeName());
            metaTypePojo.setObjectType(subMetaEnum.getObjectType());
            metaDashboardPojoList.add(metaTypePojo);
        }
        return metaDashboardPojoList;
    }

    /**
     * PostgreSQL 关键字
     * @return
     */
    public static String getPostgreSQLKeywords(){
        // Reserved keywords in PostgreSQL
        String keywords = "ALL ANALYSE ANALYZE AND ANY ARRAY AS ASC ASYMMETRIC AUTHORIZATION BINARY" +
                " BOTH CASE CAST CHECK CMAX CMIN COLLATE COLLATION COLUMN CONCURRENTLY CONSTRAINT CREATE" +
                " CROSS CTID CURRENT_CATALOG CURRENT_DATE CURRENT_ROLE CURRENT_SCHEMA CURRENT_TIME" +
                " CURRENT_TIMESTAMP CURRENT_USER DEFAULT DEFERRABLE DESC DISTINCT DO ELSE END" +
                " EXCEPT FALSE FETCH FOR FOREIGN FREEZE FROM FULL GRANT GROUP HAVING ILIKE IN" +
                " INITIALLY INNER INTERSECT INTO IS ISNULL JOIN KEY LATERAL LEADING LEFT LIKE LIMIT" +
                " LOCALTIME LOCALTIMESTAMP NATURAL NOT NOTNULL NULL OFFSET ON ONLY OR ORDER OUTER" +
                " OVERLAPS PARTITION PLACING PRIMARY REFERENCES REF RETURNING RIGHT SELECT SESSION_USER" +
                " SIMILAR SOME SYMMETRIC TABLE TABLESAMPLE THEN TO TRAILING TRUE UNION UNIQUE USER" +
                " USING VARIADIC VERBOSE WHEN WHERE WINDOW WITH";
        return keywords;
    }

    /**
     * 判断表名、表字段是否需要加双引号
     * @param tableColName
     * @return
     */
    public static boolean colNameIsNormal(String tableColName){
        //匹配不是(不区分大小写)a-z、0-9、_、$之间的任意一个字符
        if (Pattern.compile("(?i)[^a-z0-9\\_\\$]").matcher(tableColName).find()){
            return true;
        }
        //包含大小写
        boolean hasUppercase = tableColName.matches(".*[A-Z].*");
        boolean hasLowercase = tableColName.matches(".*[a-z].*");
        boolean ifUlCase = hasUppercase && hasLowercase;
        if (ifUlCase){
            return true;
        }
        //是关键字
        List<String> keywordList = Arrays.asList(getPostgreSQLKeywords().split(" "));
        boolean ifExist = keywordList.stream().anyMatch(name -> name.equalsIgnoreCase(tableColName));
        if (ifExist){
            return true;
        }
        //根据PostgreSQL的命名规则,数据库对象（如表名、字段名等）的命名不能以 数字 (0-9)、美元符号 ($) 开头。
        if(tableColName.matches("^[0-9$].*")){
            return true;
        }
        return false;
    }

    /**
     * 针对Oracle的处理
     * @param tableColName
     * @return
     */
    public static boolean oraColNameIsNormal(String tableColName){
        //包含大小写
        boolean hasUppercase = tableColName.matches(".*[A-Z].*");
        boolean hasLowercase = tableColName.matches(".*[a-z].*");
        boolean ifUlCase = hasUppercase && hasLowercase;
        if (ifUlCase == true || hasLowercase == true){
            return true;
        }
        return false;
    }

    /**
     * 处理分区表的Range
     * @param HIGH_VALUE
     * @return
     */
    public static String disposePartRange(String HIGH_VALUE){
        String new_value = HIGH_VALUE;
        if(new_value.contains("TO_DATE")) {
            String regexc = "(?<=\')([^\\.])(.*?)(?=\')";
            Pattern pattern = Pattern.compile(regexc);
            Matcher matcher = pattern.matcher(new_value);
            if (matcher.find()) {
                new_value = "\'" + matcher.group(0) + '\'';
            }
        }
        return new_value;
    }

    /**
     * 处理分区表,根据分区类型拼接语句
     * @param PARTITIONING_TYPE
     * @param HIGH_VALUE
     * @param old_value
     * @param partitionCount
     * @param series
     * @param create_table_tmp
     * @return
     */
    public static Map<String, String> disposePartPartitioningTypeSQL(String PARTITIONING_TYPE, String HIGH_VALUE, String old_value, int partitionCount, int series, String create_table_tmp){
        Map<String, String> map = new HashMap<>();
        String new_value = "";
        //根据分区类型变化
        if(PARTITIONING_TYPE.equals("LIST")){
            if(HIGH_VALUE.equalsIgnoreCase("DEFAULT")){
                create_table_tmp = create_table_tmp + "DEFAULT;\n";
            } else{
                create_table_tmp = create_table_tmp + "FOR VALUES IN ("+HIGH_VALUE+");\n";
            }
        } else if(PARTITIONING_TYPE.equals("RANGE")){
            new_value = disposePartRange(HIGH_VALUE);
            create_table_tmp = create_table_tmp + "FOR VALUES FROM ("+old_value+") TO ("+new_value+");\n";
        } else if(PARTITIONING_TYPE.equals("HASH")){
            create_table_tmp = create_table_tmp + "FOR VALUES WITH (MODULUS "+partitionCount+",REMAINDER "+(series)+");\n";
        }
        map.put("new_value", new_value);
        map.put("create_table_tmp", create_table_tmp);
        return map;
    }

    /**
     * 处理语句中的特殊字符或中文,加上双引号并且转义
     * @param sqlText
     * @return
     */
    public static String disposeSqlColNameIsNormal(String sqlText){
        String regexc = "\"(.*?)\"";
        Pattern pattern = Pattern.compile(regexc);
        Matcher matcher = pattern.matcher(sqlText);
        //存储已经替换过的字符,防止重名的重复替换
        //List<String> existList = new ArrayList<>();
        while (matcher.find()) {
            String colName = matcher.group().replaceAll("\"", "");
            if (!SubMetaUtil.colNameIsNormal(colName)) {
                sqlText = sqlText.replaceAll("\"" + colName + "\"", colName);
            }
        }
        sqlText = sqlText.replaceAll("\"","\\\\\"");
        return sqlText;
    }

    /**
     * 使用List方法removeAll()删除与另一个列表共有的所有元素
     * @return
     */
    public static List<String> listRemoveSame(List<String> list1,List<String> list2){
        // List<String> tmp = new ArrayList<>(list1);
        // tmp.removeAll(list2);
        // return tmp;
        Iterator<String> iterator = list1.iterator();
        while (iterator.hasNext()) {
            String item = iterator.next();
            if (list2.contains(item)) {
                list2.remove(item); // 从list2中移除已经匹配的元素，确保每个元素只匹配一次
                iterator.remove(); // 从list1中移除这个元素
            }
        }
        return list1;
    }

    /**
     * 找到两个列表之间的共同元素
     * @return
     */
    public static List<String> listRetain(List<String> list1,List<String> list2){
        List<String> tmp = new ArrayList<>(list1);
        tmp.retainAll(list2);
        return tmp;
    }

    public static String modifyName(String old, String regex){
        return old.replaceAll(regex, "");
    }

    /**
     * 生成Kafka任务的Json
     * @param map
     * @param datasource
     * @return
     */
    public static JSONObject disposeSyncJson(Map<String, String> map, JobDatasource datasource) {
        String datasourceType = datasource.getDatasource();
        //createType: reader/writer
        String createType = map.get("createType");
        String schema = map.get("schema");
        String tableName = map.get("tableName");
        String oracleVersion = map.get("oracleVersion");
        String rePdbName = map.get("rePdbName");
        String reServerId = map.get("reServerId");
        String reServerName = map.get("reServerName");
        String reKeyColumns = map.get("reKeyColumns");
        String[] keyColumns = reKeyColumns.split(",");
        //提取字符串中的ip+端口
        Map<String, String> urlMap = getUrlIpPort(datasource.getJdbcUrl());
        String ip = urlMap.get("ip");
        String port = urlMap.get("port");
        //生成唯一id
        long syncId = new Snowflake(1,2).nextId();
        String url = datasource.getJdbcUrl();
        String user = AESUtil.decrypt(datasource.getJdbcUsername());
        String password = AESUtil.decrypt(datasource.getJdbcPassword());
        String databaseName = url.substring(url.indexOf(port+"/") + 5);
        if(datasourceType.equals("sqlserver")){
            databaseName = url.substring(url.indexOf("DatabaseName=") + 13);
        }
        String topics = databaseName;
        String name = ip.substring(ip.lastIndexOf('.') + 1);
        JSONObject jsonObject = new JSONObject();
        //构建json
        if(createType.equals("reader")){
            if(tableName != null && !tableName.equals("")){
                topics = "so-" + datasourceType + "-" + name + "-" + topics + "." + tableName;
            }
            if(datasourceType.equals("oracle")){
                if (oracleVersion.equals("oracle19")) {
                    String json = getFileText("kfk-config/source-oracle-19");
                    jsonObject = JSON.parseObject(json);
                    jsonObject.put("name", "so-oracle19-" + name + "-" + syncId);
                    if (!rePdbName.equals("")){
                        jsonObject.getJSONObject("config").put("database.pdb.name", rePdbName);
                    }
                } else if (oracleVersion.equals("oracle11")) {
                    String json = getFileText("kfk-config/source-oracle-11");
                    jsonObject = JSON.parseObject(json);
                    jsonObject.put("name", "so-oracle11-" + name + "-" + syncId);
                    jsonObject.getJSONObject("config").put("schema.include.list", schema);
                }
            } else {
                String json = getFileText("kfk-config/source-" + datasourceType);
                jsonObject = JSON.parseObject(json);
                jsonObject.put("name", "so-" + datasourceType + "-" + name + "-" + syncId);
            }
            if(keyColumns.length > 0 && !reKeyColumns.equals("")){
                StringBuilder columnsText = new StringBuilder();
                for (String keyColumn:keyColumns) {
                    if(datasourceType.equals("mysql")){
                        columnsText.append(schema).append(".").append(tableName).append(":").append(keyColumn).append(";");
                    } else{
                        columnsText.append(tableName).append(":").append(keyColumn).append(";");
                    }
                }
                jsonObject.getJSONObject("config").put("message.key.columns", columnsText.toString());
            }
            jsonObject.getJSONObject("config").put("database.user", user);
            jsonObject.getJSONObject("config").put("database.password", password);
            jsonObject.getJSONObject("config").put("database.hostname", ip);
            jsonObject.getJSONObject("config").put("database.port", port);
            jsonObject.getJSONObject("config").put("database.server.name", reServerName);
            if(datasourceType.equals("mysql")){
                jsonObject.getJSONObject("config").put("database.include.list", schema);
                jsonObject.getJSONObject("config").put("table.include.list", schema + "." + tableName);
                jsonObject.getJSONObject("config").put("database.server.id", reServerId);
            } else{
                jsonObject.getJSONObject("config").put("database.dbname", databaseName);
                jsonObject.getJSONObject("config").put("table.include.list", tableName);
            }
            //Halo此处去掉connector.class,下发的时候会重新加上
            if(datasourceType.equals("halo")){
                jsonObject.getJSONObject("config").remove("connector.class");
            }
            jsonObject.getJSONObject("config").put("database.history.kafka.topic", topics);
            jsonObject.getJSONObject("config").put("database.history.kafka.bootstrap.servers", JobAdminConfig.getAdminConfig().getKafkaHost());
        } else if(createType.equals("writer")){
            String readerDatasource = map.get("readerDatasource");
            String json = getFileText("kfk-config/sink-" + datasourceType);
            jsonObject = JSON.parseObject(json);
            if(tableName != null && !tableName.equals("")){
                // 源端如果是Halo，则topics中的schema和表名需要转换成小写
                // 源端如果是Oracle、Db2，则topics中的schema和表名需要转换成大写
                if(readerDatasource.equals("halo")){
                    topics = topics + "." + tableName.toLowerCase();
                } else if(readerDatasource.equals("oracle") || readerDatasource.equals("db2")){
                    topics = topics + "." + tableName.toUpperCase();
                } else{
                    topics = topics + "." + tableName;
                }
            }
            jsonObject.put("name", "si-" + datasourceType + "-" + name + "-" + syncId);
            jsonObject.getJSONObject("config").put("connection.url", url);
            jsonObject.getJSONObject("config").put("connection.user", user);
            jsonObject.getJSONObject("config").put("connection.password", password);
            if(datasourceType.equals("mysql")){
                jsonObject.getJSONObject("config").put("topics", databaseName + "." + topics);
                jsonObject.getJSONObject("config").put("table.name.format", databaseName + "." + tableName);
            } else if(datasourceType.equals("sqlserver")){
                jsonObject.getJSONObject("config").put("topics", topics);
                jsonObject.getJSONObject("config").put("table.name.format", databaseName + "." + tableName);
            } else{
                jsonObject.getJSONObject("config").put("topics", topics);
                jsonObject.getJSONObject("config").put("table.name.format", tableName);
            }
        }
        return jsonObject;
    }

    /**
     * 提取字符串中的ip+端口
     * @param url
     * @return
     */
    public static Map<String, String> getUrlIpPort(String url){
        Map<String, String> map = new HashMap<>();
        //校验地址中是否存在 “ip:端口号”  （例如rtsp://admin:admin@192.168.30.98:554/media/video1 ）
        Pattern p = Pattern.compile("(\\d+\\.\\d+\\.\\d+\\.\\d+)\\:(\\d+)");
        Matcher m = p.matcher(url);
        //将符合规则的提取出来
        while(m.find()) {
            map.put("ip", m.group(1));
            map.put("port", m.group(2));
        }
        return map;
    }

    /**
     * 获取resources文件下指定文件内容
     * @param fileName
     * @return
     */
    public static String getFileText(String fileName){
        StringBuilder fileText = new StringBuilder();
        // 通过ClassLoader读取resources下的文件
        InputStream inputStream = ResourceReader.class.getClassLoader().getResourceAsStream(fileName);
        if (inputStream != null) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    fileText.append(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fileText.toString();
    }

    /**
     * 拼接简易表结构
     * @param tableColumnsList
     * @return
     */
    public static String createTableSQL(String tableName, List<Map<String, String>> tableColumnsList){
        if(!tableColumnsList.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE ");
            sb.append(tableName);
            sb.append("(");
            for (Map<String, String> map : tableColumnsList) {
                sb.append("\n").append("  ");
                sb.append(map.get("COLUMN_NAME"));
                sb.append(" ");
                sb.append(map.get("DATA_TYPE"));
                String dataLength = map.get("DATA_LENGTH");
                if(dataLength != null && !dataLength.equals("")){
                    sb.append("(").append(dataLength).append(")");
                }
                sb.append(",");
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append("\n").append(");");
            return sb.toString();
        } else{
            return "";
        }
    }

    /**
     * 拼接简易表结构
     * @param tableColumnsList
     * @return
     */
    public static Map<String, String> createMySQLTableSQL(String typeName, String tableName, List<Map<String, String>> tableColumnsList, List<Map<String, String>> indexList, List<Map<String, String>> foreignKeyList){
        if(!tableColumnsList.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("CREATE TABLE ");
            sb.append(tableName);
            sb.append("(");
            //字段、数据类型、精度、是否非空、默认值 拼接
            for (Map<String, String> map : tableColumnsList) {
                sb.append("\n").append("  ");
                sb.append(map.get("COLUMN_NAME"));
                sb.append(" ");
                sb.append(map.get("DATA_TYPE"));
                String dataLength = map.get("DATA_LENGTH");
                if(dataLength != null && !dataLength.equals("")){
                    sb.append("(").append(dataLength).append(")");
                }
                if(map.get("IS_NULLABLE").equals("NO")){
                    sb.append(" ");
                    sb.append("NOT NULL");
                }
                if(map.get("COLUMN_DEFAULT") != null){
                    sb.append(" ");
                    if (typeName.equals("reader")){
                        String COLUMN_DEFAULT = map.get("COLUMN_DEFAULT");
                        // 遇到CURRENT_TIMESTAMP不加单引号,和halo一致
                        if(map.get("COLUMN_DEFAULT").contains("CURRENT_TIMESTAMP")){
                            sb.append("DEFAULT " + COLUMN_DEFAULT);
                        } else{
                            sb.append("DEFAULT '" + COLUMN_DEFAULT + "'");
                        }
                    } else{
                        String COLUMN_DEFAULT = map.get("COLUMN_DEFAULT");
                        //mapping代表映射前的语法
                        if (typeName.equals("mapping")){
                            //遇到int加上单引号,和MySQL一致
                            if (map.get("DATA_TYPE").equals("int")){
                                if(COLUMN_DEFAULT.contains("'")){
                                    //以防本身就有单引号
                                } else{
                                    COLUMN_DEFAULT = "'" + COLUMN_DEFAULT + "'";
                                }
                            }
                            //遇到bit(1)则改为以下方式,和MySQL一致
                            if (map.get("DATA_TYPE").equals("bit") && map.get("DATA_LENGTH").equals("1")){
                                COLUMN_DEFAULT = "'b'0''";
                            }
                            //暂对mysql内核迁移做兼容对比
                            if(COLUMN_DEFAULT.contains("LOCALTIMESTAMP")){
                                //mysql不会加上(0)
                                if(COLUMN_DEFAULT.equals("LOCALTIMESTAMP(0)")){
                                    COLUMN_DEFAULT = "CURRENT_TIMESTAMP";
                                } else{
                                    //LOCALTIMESTAMP(3) - CURRENT_TIMESTAMP(3)
                                    COLUMN_DEFAULT = COLUMN_DEFAULT.replaceAll("LOCALTIMESTAMP", "CURRENT_TIMESTAMP");
                                }
                            }
                            if(COLUMN_DEFAULT.contains("NULL") || COLUMN_DEFAULT.contains("nextval")){
                                // 删除最后的空格
                                sb.setLength(sb.length() - 1);
                            } else{
                                //将::后面包括自身的所有内容全部删除
                                sb.append("DEFAULT " + COLUMN_DEFAULT.replaceAll("::.*", ""));
                            }
                            //将::后面包括自身的所有内容全部删除
                            //sb.append("DEFAULT " + COLUMN_DEFAULT.replaceAll("::.*", ""));
                        } else{
                            sb.append("DEFAULT " + COLUMN_DEFAULT);
                        }
                    }
                }
                sb.append(",");
            }
            //索引+外键 另外用于字符对比
            StringBuilder other = new StringBuilder();
            //索引 拼接
            if(!indexList.isEmpty()){
                if(typeName.equals("reader")){
                    //先根据索引名称分组
                    Map<String, List<Map<String, String>>> groupByIndexNameMap = indexList.stream().collect(Collectors.groupingBy(doc -> doc.get("INDEX_NAME"),LinkedHashMap::new,Collectors.toList())); //此处使用LinkedHashMap保证原有的元素顺序
                    groupByIndexNameMap.forEach((indexNameMapK, indexNameMapV) -> {
                        //表名
                        //String tableName = indexNameMapV.get(0).get("TABLE_NAME");
                        //索引名称
                        String indexName = indexNameMapV.get(0).get("INDEX_NAME");
                        //是否唯一: 唯一0 不唯一1
                        String nonUnique = indexNameMapV.get(0).get("NON_UNIQUE");
                        //字段
                        String fields = "";
                        for (Map<String, String> map : indexNameMapV) {
                            //列名
                            String columnName = map.get("COLUMN_NAME");
                            fields = fields + columnName + ",";
                        }
                        if(nonUnique.equals("0")){
                            String sqlText = "PRIMARY KEY ("+fields.substring(0, fields.length()-1)+")";
                            other.append("\n").append("  ");
                            other.append(sqlText).append(",");
                        } else {
                            String indexNewName = indexName;
                            String sqlText = "KEY "+indexNewName+" ("+fields.substring(0, fields.length()-1)+")";
                            other.append("\n").append("  ");
                            other.append(sqlText).append(",");
                        }
                    });
                } else{
                    for (Map<String, String> indexMap : indexList) {
                        //索引名称
                        String indexName = indexMap.get("indexname");
                        //是否唯一: 唯一t 不唯一f
                        String nonUnique = indexMap.get("indisunique");
                        //定义语句
                        String definition = indexMap.get("definition");
                        if(nonUnique.equals("t")){
                            //只取括号里面的参数(id_1,id_2)
                            String sqlText = "PRIMARY KEY ("+definition.replaceAll(".*\\(([^)]*)\\).*","$1").replaceAll(" ","")+")";
                            other.append("\n").append("  ");
                            other.append(sqlText).append(",");
                        } else {
                            String indexNewName = indexName;
                            if(typeName.equals("mapping")){
                                //暂兼容mysql,内核那边是把表的oid拼接_oid_这样的,此处暂时去掉2位或以上的任意数字
                                indexNewName = indexNewName.replaceAll("_\\d{2,}_", "");
                                //暂时将匹配了_后面跟着的6位数字全部删除,作用是把索引名称转换成转换前,但是此处不严谨,需要定义更详细的规约,比如_halo1001
                                indexNewName = indexNewName.replaceAll("_\\d{6}.*", ""); 
                            }
                            String sqlText = "KEY "+indexNewName+" ("+definition.replaceAll(".*\\(([^)]*)\\).*","$1").replaceAll(" ","")+")";
                            other.append("\n").append("  ");
                            other.append(sqlText).append(",");
                        } 
                    }
                }
            }
            //外键 拼接
            if(!foreignKeyList.isEmpty()){
                if(typeName.equals("reader")){
                    Map<String, List<Map<String, String>>> groupByF_Map = foreignKeyList.stream().collect(Collectors.groupingBy(doc -> doc.get("CONSTRAINT_NAME")));
                    groupByF_Map.forEach((k, v) -> {
                        //表名
                        //String tableName = v.get(0).get("TABLE_NAME");
                        //引用的表名
                        String referencedTableName = v.get(0).get("REFERENCED_TABLE_NAME");
                        //列名
                        String columnName = "";
                        //引用的列名
                        String referencedColumnName = "";
                        //引用的schema
                        String reTableSchema =  v.get(0).get("REFERENCED_TABLE_SCHEMA");
                        for (Map<String, String> map : v){
                            String columnNameV = map.get("COLUMN_NAME"); 
                            columnName = columnName + columnNameV + ",";
                            String referencedColumnNameV = map.get("REFERENCED_COLUMN_NAME"); 
                            referencedColumnName = referencedColumnName + referencedColumnNameV + ",";
                        }
                        //指定不同的删除和更新行为
                        String deleteRule = v.get(0).get("DELETE_RULE");
                        String updateRule = v.get(0).get("UPDATE_RULE");
                        //约束名称
                        String constraintName = v.get(0).get("CONSTRAINT_NAME");
                        String sqlText = "CONSTRAINT "+constraintName+" FOREIGN KEY ("+columnName.replaceAll(",+$", "")+") REFERENCES "+reTableSchema+"."+referencedTableName+"("+referencedColumnName.replaceAll(",+$", "")+") ON UPDATE "+updateRule+" ON DELETE "+deleteRule+"";
                        other.append("\n").append("  ");
                        other.append(sqlText).append(",");
                    });
                } else{
                    for (Map<String, String> foreignKeyMap : foreignKeyList) {
                        //外键约束名称
                        String constraintName = foreignKeyMap.get("constraint_name");
                        //定义语句
                        String definition = foreignKeyMap.get("constraint_definition") + "";
                        //指定对应表
                        //String foreignTableName = foreignKeyMap.get("foreign_table_name");
                        String sqlText = "CONSTRAINT "+constraintName+" "+removeSpacesInParentheses(definition)+"";
                        other.append("\n").append("  ");
                        other.append(sqlText).append(",");
                    }
                }
            }
            sb.append(other.toString());
            sb.deleteCharAt(sb.length() - 1);
            sb.append("\n").append(");");
            Map<String, String> map = new HashMap<>();
            map.put("sqlText", sb.toString());
            map.put("otherSqlText", other.toString());
            return map;
        } else{
            Map<String, String> map = new HashMap<>();
            map.put("sqlText", "");
            map.put("otherSqlText", "");
            return map;
        }
    }
    

    /**
     * 用于Oracle和Halo的数据类型对比
     * @param oracleDataType
     * @param haloDataType
     * @param jobDataTypeMappingList
     * @return
     */
    // public static String getOracleAndHaloDataTypeMapping(String oracleDataType, String haloDataType, List<JobDataTypeMapping> jobDataTypeMappingList){
    //     String dataTypes = "";
    //     List<String> dataTypeList = null;
    //     List<JobDataTypeMapping> dataTypeMappingList = jobDataTypeMappingList.stream().filter(j -> j.getWriterDataType().equals(haloDataType)).collect(Collectors.toList());
    //     //优先使用设置过的数据类型映射进行对比
    //     if (!dataTypeMappingList.isEmpty()){
    //         dataTypeList = dataTypeMappingList.stream().map(JobDataTypeMapping::getReaderDataType).collect(Collectors.toList());
    //     } else{
    //         switch (haloDataType) {
    //             case "character varying":
    //                 dataTypes = "VARCHAR NVARCHAR VARCHAR2 NVARCHAR2";
    //                 break;
    //             case "numeric":
    //                 dataTypes = "NUMBER FLOAT";
    //                 break;
    //             case "character":
    //                 dataTypes = "CHAR NCHAR";
    //                 break;
    //             case "text":
    //                 dataTypes = "CLOB NCLOB LONG";
    //                 break;
    //             case "bytea":
    //                 dataTypes = "BLOB";
    //                 break;
    //             case "timestamp without time zone":
    //                 // dataTypes = "TIMESTAMP DATE";
    //                 break;
    //             default:
    //                 break;
    //         }
    //         dataTypeList = Arrays.asList(dataTypes.split(" "));
    //     }
    //     boolean ifExist = dataTypeList.stream().anyMatch(name -> name.equals(oracleDataType));
    //     if(ifExist){
    //         return oracleDataType;
    //     }
    //     return haloDataType;
    // }
    
    public static String getOracleAndHaloDataTypeMapping(String oracleDataType, String haloDataType, List<JobDataTypeMapping> jobDataTypeMappingList) {
        // 优先使用设置过的数据类型映射进行对比
        List<JobDataTypeMapping> dataTypeMappingList = jobDataTypeMappingList.stream().filter(j -> j.getWriterDataType().equals(haloDataType)).collect(Collectors.toList());
    
        // 如果存在映射，直接获取映射列表
        if (!dataTypeMappingList.isEmpty()) {
            return dataTypeMappingList.stream().map(JobDataTypeMapping::getReaderDataType).filter(readerDataType -> readerDataType.equals(oracleDataType)).findFirst().orElse(haloDataType);  // 如果没有找到匹配的，返回原始 haloDataType
        }
    
        // 如果没有设置映射，则使用默认映射逻辑
        Set<String> dataTypeSet = new HashSet<>();
        switch (haloDataType) {
            case "character varying":
                dataTypeSet.addAll(Arrays.asList("VARCHAR", "NVARCHAR", "VARCHAR2", "NVARCHAR2"));
                break;
            case "numeric":
                dataTypeSet.addAll(Arrays.asList("NUMBER", "FLOAT"));
                break;
            case "character":
                dataTypeSet.addAll(Arrays.asList("CHAR", "NCHAR"));
                break;
            case "text":
                dataTypeSet.addAll(Arrays.asList("CLOB", "NCLOB", "LONG"));
                break;
            case "bytea":
                dataTypeSet.add("BLOB");
                break;
            case "xml":
                dataTypeSet.add("XMLTYPE");
                break;
            case "USER-DEFINED":
                dataTypeSet.add("RAW");
                break;
            case "timestamp without time zone":
                // 如果是timestamp without time zone，返回TIMESTAMP和带精度的TIMESTAMP
                for (int i = 0; i <= 9; i++) {
                    dataTypeSet.add("TIMESTAMP(" + i + ")");
                }
                dataTypeSet.add("TIMESTAMP");  // 普通的TIMESTAMP类型
                dataTypeSet.add("DATE");       // DATE类型
                break;
            case "timestamp with time zone":
                // 同时支持 WITH TIME ZONE 变种
                for (int i = 0; i <= 9; i++) {
                    dataTypeSet.add("TIMESTAMP(" + i + ") WITH TIME ZONE");
                }
                dataTypeSet.add("TIMESTAMP WITH TIME ZONE");
                dataTypeSet.add("TIMESTAMP WITH LOCAL TIME ZONE"); // 如果需要支持 LOCAL TIME ZONE
                break;
            default:
                break;
        }

        // 判断oracleDataType是否在映射列表中
        if (dataTypeSet.contains(oracleDataType)) {
            return oracleDataType;
        }
        return haloDataType;
    }


    public static String getMySQLAndHaloDataTypeMapping(String oracleDataType, String haloDataType, List<JobDataTypeMapping> jobDataTypeMappingList){
        String dataTypes = "";
        List<String> dataTypeList = new ArrayList<>();
        List<JobDataTypeMapping> dataTypeMappingList = jobDataTypeMappingList.stream().filter(j -> j.getWriterDataType().equals(haloDataType)).collect(Collectors.toList());
        //优先使用设置过的数据类型映射进行对比
        if (!dataTypeMappingList.isEmpty()){
            dataTypeList = dataTypeMappingList.stream().map(JobDataTypeMapping::getReaderDataType).collect(Collectors.toList());
        }
        switch (haloDataType) {
            case "character varying":
                dataTypes = "varchar";
                break;
            case "integer":
                dataTypes = "int";
                break;
            case "bigint":
                dataTypes = "int";
                break;
            case "timestamp without time zone":
                dataTypes = "timestamp";
                break;
            case "numeric":
                dataTypes = "decimal";
                break;
            case "character":
                dataTypes = "char";
                break;
            default:
                break;
        }
        List<String> dataTypeList2 = Arrays.asList(dataTypes.split(" "));
        if (!dataTypeList2.isEmpty()){
            dataTypeList.addAll(dataTypeList2);
        }
        boolean ifExist = dataTypeList.stream().anyMatch(name -> name.equals(oracleDataType));
        if(ifExist){
            return oracleDataType;
        }
        return haloDataType;
    }

    /**  
     * 移除字符串中圆括号内部的所有空格。  
     *  
     * @param input 输入字符串  
     * @return 处理后的字符串  
     */  
    public static String removeSpacesInParentheses(String input) {  
        if (input == null) {  
            return null;  
        }  
        // 定义正则表达式来匹配圆括号及其内部内容  
        Pattern pattern = Pattern.compile("\\(([^\\)]*)\\)");  
        Matcher matcher = pattern.matcher(input);  
        // 使用StringBuffer来构建结果字符串，因为String是不可变的  
        StringBuffer sb = new StringBuffer();  
        // 遍历所有匹配项  
        while (matcher.find()) {  
            // 移除捕获组中的空格  
            String group = matcher.group(1).replaceAll("\\s+", "");  
            // 使用替换后的字符串替换原始匹配项  
            matcher.appendReplacement(sb, "(" + group + ")");  
        }  
        // 添加最后一段未匹配的内容（如果有的话）  
        matcher.appendTail(sb);  
        return sb.toString();  
    } 

}
