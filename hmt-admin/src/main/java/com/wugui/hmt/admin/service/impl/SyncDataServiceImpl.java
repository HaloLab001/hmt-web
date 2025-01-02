package com.wugui.hmt.admin.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.admin.core.conf.JobAdminConfig;
import com.wugui.hmt.admin.dto.SyncJsonBuildDto;
import com.wugui.hmt.admin.entity.JobDatasource;
import com.wugui.hmt.admin.entity.JobKafkaSync;
import com.wugui.hmt.admin.mapper.KafkaSyncMapper;
import com.wugui.hmt.admin.service.DatasourceQueryService;
import com.wugui.hmt.admin.service.JobDatasourceService;
import com.wugui.hmt.admin.service.SubMetaDataService;
import com.wugui.hmt.admin.service.SyncDataService;
import com.wugui.hmt.admin.util.SubMetaUtil;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;

/*-------------------------------------------------------------------------
 *
 * SyncDataServiceImpl.java
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/service/impl/SyncDataServiceImpl.java
 *
 *-----------------------------------------------
 */
@Service
public class SyncDataServiceImpl implements SyncDataService {

    @Autowired
    private JobDatasourceService jobDatasourceService;

    @Autowired
    private KafkaSyncMapper kafkaSyncMapper;

    @Autowired
    private DatasourceQueryService datasourceQueryService;

    @Override
    public JSONArray getConnectors() {
        String result = executeHttpURLConnectionToGet(JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors");
        return (JSONArray)JSONArray.parse(result);
    }

    @Override
    public JSONObject getConnectorsStatus(String name) {
        String result = executeHttpURLConnectionToGet(JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors/"+name+"/status");
        return JSON.parseObject(result);
    }

    @Override
    public JSONObject getStatusApiAddress(String name) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("api", JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors/"+name+"/status");
        return jsonObject;
    }

    @Override
    public JSONObject createJson(Map<String, String> map) {
        Long datasourceId = Long.valueOf(String.valueOf(map.get("datasourceId")));
        //获取数据源对象
        JobDatasource datasource = jobDatasourceService.getById(datasourceId);
        return SubMetaUtil.disposeSyncJson(map, datasource);
    }

    @Override
    public ReturnT<JSONObject> saveTask(SyncJsonBuildDto syncJsonBuildDto) {
        String result = "";
        //修改任务的情况下
        if(syncJsonBuildDto.getTaskId() > 0){
            //查询任务信息
            JobKafkaSync queryKafkaSync = kafkaSyncMapper.getInfoById(syncJsonBuildDto.getTaskId());
            String queryTaskName = null;
            String syncTaskName = null;
            if(syncJsonBuildDto.getSaveTypeName().equals("reader")) {
                if(!queryKafkaSync.getReaderJson().equals("{}")) {
                    queryTaskName = JSONObject.parseObject(queryKafkaSync.getReaderJson()).getString("name");
                    syncTaskName = JSONObject.parseObject(syncJsonBuildDto.getReaderJson()).getString("name");
                }
            } else{
                if(!queryKafkaSync.getWriterJson().equals("{}")) {
                    queryTaskName = JSONObject.parseObject(queryKafkaSync.getWriterJson()).getString("name");
                    syncTaskName = JSONObject.parseObject(syncJsonBuildDto.getWriterJson()).getString("name");
                }
            }
            if(queryTaskName != null && !queryTaskName.equals(syncTaskName)){
                executeCurl(JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors/", queryTaskName, "DELETE");
            }
        }
        //由于Halo的json去掉了connector.class，此处下发到kafka重新加上
        if(syncJsonBuildDto.getSaveTypeName().equals("reader") && syncJsonBuildDto.getReaderDatasource().equals("halo")){
            JSONObject jsonObject = JSON.parseObject(syncJsonBuildDto.getReaderJson());
            jsonObject.getJSONObject("config").put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
            syncJsonBuildDto.setReaderJson(jsonObject.toJSONString());
        }
        //下发命令
        if(syncJsonBuildDto.getSaveTypeName().equals("reader")){
            executeCurl(JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors/", JSONObject.parseObject(syncJsonBuildDto.getReaderJson()).getString("name"), "DELETE");
            result = executeCurl(JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors/", syncJsonBuildDto.getReaderJson(), "POST");
        } else{
            executeCurl(JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors/", JSONObject.parseObject(syncJsonBuildDto.getWriterJson()).getString("name"), "DELETE");
            result = executeCurl(JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors/", syncJsonBuildDto.getWriterJson(), "POST");
        }
        JSONObject jsonObject = JSONObject.parseObject(result);
        if(jsonObject.containsKey("error_code")){
            return new ReturnT<>(ReturnT.FAIL_CODE, jsonObject.getString("message"));
        }
        //构建json
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("readerDatasourceId", syncJsonBuildDto.getReaderDatasourceId());
        jsonMap.put("readerDatasource", syncJsonBuildDto.getReaderDatasource());
        jsonMap.put("readerTableSchema", syncJsonBuildDto.getReaderTableSchema());
        jsonMap.put("readerTableName", syncJsonBuildDto.getReaderTableName());
        jsonMap.put("reServerName", syncJsonBuildDto.getReServerName());
        jsonMap.put("reServerId", syncJsonBuildDto.getReServerId());
        jsonMap.put("oracleVersion", syncJsonBuildDto.getOracleVersion());
        jsonMap.put("rePdbName", syncJsonBuildDto.getRePdbName());

        jsonMap.put("reKeyColumns", syncJsonBuildDto.getReKeyColumns());
        jsonMap.put("writerDatasourceId", syncJsonBuildDto.getWriterDatasourceId());
        jsonMap.put("writerDatasource", syncJsonBuildDto.getWriterDatasource());
        jsonMap.put("writerTableSchema", syncJsonBuildDto.getWriterTableSchema());
        jsonMap.put("writerTableName", syncJsonBuildDto.getWriterTableName());
        JobKafkaSync kafkaSync = new JobKafkaSync();
        kafkaSync.setId(syncJsonBuildDto.getTaskId());
        //对任务名称进行处理
        JobDatasource datasource = jobDatasourceService.getById(syncJsonBuildDto.getReaderDatasourceId());
        String url = datasource.getJdbcUrl();
        String port = SubMetaUtil.getUrlIpPort(url).get("port");
        String databaseName = url.substring(url.indexOf(port+"/") + 5);
        if(syncJsonBuildDto.getReaderDatasource().equals("sqlserver")){
            databaseName = url.substring(url.indexOf("DatabaseName=") + 13);
        }
        String taskName = syncJsonBuildDto.getTaskName();
        if(taskName.equals("")){
            if(syncJsonBuildDto.getReaderDatasource().equals("mysql")) {
                taskName = syncJsonBuildDto.getReaderDatasource() + "." + databaseName + "." + syncJsonBuildDto.getReaderTableSchema() + "." + syncJsonBuildDto.getReaderTableName();
            }else{
                taskName = syncJsonBuildDto.getReaderDatasource() + "." + databaseName + "." + syncJsonBuildDto.getReaderTableName();
            }
        }
        kafkaSync.setSyncName(taskName);
        kafkaSync.setReaderJson(syncJsonBuildDto.getReaderJson());
        kafkaSync.setWriterJson(syncJsonBuildDto.getWriterJson());
        kafkaSync.setSyncJson(JSON.toJSONString(jsonMap));
        kafkaSync.setProjectId(syncJsonBuildDto.getProjectId());
        int taskId = 0;
        //新增/修改
        if(syncJsonBuildDto.getTaskId() <= 0){
            kafkaSyncMapper.save(kafkaSync);
            taskId = kafkaSync.getId();
        } else{
            kafkaSyncMapper.update(kafkaSync);
            taskId = kafkaSync.getId();
        }
        JSONObject resultObject = new JSONObject();
        resultObject.put("taskId", taskId);
        return new ReturnT<>(resultObject);
    }

    @Override
    public Map<String, Object> pageList(int start, int length, String taskName, Integer[] projectIds) {
        // page list
        List<JobKafkaSync> list = kafkaSyncMapper.pageList(start, length, taskName, projectIds);
        int list_count = kafkaSyncMapper.pageListCount(start, length, taskName, projectIds);
        // package result
        Map<String, Object> maps = new HashMap<>();
        maps.put("recordsTotal", list_count);        // 总记录数
        maps.put("recordsFiltered", list_count);    // 过滤后的总记录数
        maps.put("data", list);                    // 分页列表
        return maps;
    }

    @Override
    public ReturnT<JobKafkaSync> getInfoById(int id) {
        JobKafkaSync kafkaSync = kafkaSyncMapper.getInfoById(id);
        return new ReturnT<>(kafkaSync);
    }

    @Override
    public int delete(int id) {
        JobKafkaSync kafkaSync = kafkaSyncMapper.getInfoById(id);
        executeCurl(JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors/", JSONObject.parseObject(kafkaSync.getReaderJson()).getString("name"), "DELETE");
        executeCurl(JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors/", JSONObject.parseObject(kafkaSync.getWriterJson()).getString("name"), "DELETE");
        return kafkaSyncMapper.delete(id);
    }

    @Override
    public ReturnT<Map<String, Object>> batchSaveTask(SyncJsonBuildDto syncJsonBuildDto) {
        //源端需要同步的表数组
        String[] readerTables = syncJsonBuildDto.getReaderTables();
        //查询源端schema下不包含主键的表集合
        Long readerDatasourceId = Long.parseLong(String.valueOf(syncJsonBuildDto.getReaderDatasourceId()));
        List<String> noPrimaryKeyTables = new ArrayList<>();
        //查询目标端schema下的表数组
        Long writerDatasourceId = Long.parseLong(String.valueOf(syncJsonBuildDto.getWriterDatasourceId()));
        List<String> writerTables = new ArrayList<>();
        try{
            noPrimaryKeyTables = datasourceQueryService.getNoPrimaryKeyTables(readerDatasourceId, syncJsonBuildDto.getReaderTableSchema());
            writerTables = datasourceQueryService.getTables(writerDatasourceId, syncJsonBuildDto.getWriterTableSchema());
        } catch (IOException e){
            e.printStackTrace();
        }
        String readerSchema = syncJsonBuildDto.getReaderTableSchema();
        String writerSchema = syncJsonBuildDto.getWriterTableSchema();
        JobDatasource readerDatasource = jobDatasourceService.getById(syncJsonBuildDto.getReaderDatasourceId());
        JobDatasource writerDatasource = jobDatasourceService.getById(syncJsonBuildDto.getWriterDatasourceId());
        //记录任务创建失败的表
        List<Map<String,Object>> tableNoExistList = new ArrayList<>();
        List<Map<String,Object>> tableNoKeyList = new ArrayList<>();
        //批量创建任务
        for (String readerTableName:readerTables) {
            //String readerTable = readerTableName.replaceAll(syncJsonBuildDto.getReaderTableSchema()+"\\.", "");
            String compareReaderTableName = readerTableName;
            if(readerDatasource.getDatasource().equals("mysql")){
                compareReaderTableName = writerSchema + "." +compareReaderTableName;
            } else if (writerDatasource.getDatasource().equals("mysql")){
                compareReaderTableName = compareReaderTableName.replaceAll(readerSchema+"\\.", "");
            }
            String finalCompareReaderTableName = compareReaderTableName;
            //查询目标端是否存在这张表
            Optional<String> writerTableName = writerTables.stream().filter(s -> s.equalsIgnoreCase(finalCompareReaderTableName)).findFirst();
            if(writerTableName.isPresent()){
                //先判断该表是否包含主键
                Optional<String> isNoKey = noPrimaryKeyTables.stream().filter(s -> s.equals(readerTableName.replaceAll(readerSchema+"\\.", ""))).findFirst();
                if(isNoKey.isPresent()){
                    Map<String,Object> errorTableMap = new HashMap<>();
                    errorTableMap.put("tableName", readerTableName);
                    errorTableMap.put("cause", "该表没有主键,需要单独创建任务");
                    tableNoKeyList.add(errorTableMap);
                    continue;
                }
                Map<String, String> map = new HashMap<>();
                map.put("createType", "reader");
                map.put("schema", readerSchema);
                map.put("tableName", readerTableName);
                map.put("oracleVersion", syncJsonBuildDto.getOracleVersion());
                map.put("rePdbName", syncJsonBuildDto.getRePdbName());

                map.put("reServerId", syncJsonBuildDto.getReServerId());
                map.put("reServerName", syncJsonBuildDto.getReServerName());
                map.put("reKeyColumns", "");
                map.put("readerDatasource", syncJsonBuildDto.getReaderDatasource());
                JSONObject readerJson = SubMetaUtil.disposeSyncJson(map, readerDatasource);
                //由于Halo的json去掉了connector.class，此处下发到kafka重新加上
                if(syncJsonBuildDto.getReaderDatasource().equals("halo")){
                    readerJson.getJSONObject("config").put("connector.class", "io.debezium.connector.postgresql.PostgresConnector");
                }

                map.put("createType", "writer");
                map.put("schema", writerSchema);
                map.put("tableName", writerTableName.get());
                JSONObject writerJson = SubMetaUtil.disposeSyncJson(map, writerDatasource);
                syncJsonBuildDto.setReaderJson(readerJson.toJSONString());
                syncJsonBuildDto.setWriterJson(writerJson.toJSONString());
                syncJsonBuildDto.setReaderTableName(readerTableName);
                syncJsonBuildDto.setWriterTableName(writerTableName.get());
                //下发到第三方
                String readerResult = executeCurl(JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors/", syncJsonBuildDto.getReaderJson(), "POST");
                String writerResult = executeCurl(JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors/", syncJsonBuildDto.getWriterJson(), "POST");
                //保存任务到表
                saveKafkaSyncTask(syncJsonBuildDto, readerDatasource);
            }else{
                Map<String,Object> errorTableMap = new HashMap<>();
                errorTableMap.put("tableName", readerTableName);
                errorTableMap.put("cause", "目标端不存在该表");
                tableNoExistList.add(errorTableMap);
            }
        }
        Map<String,Object> map = new HashMap<>();
        map.put("noExistTables", tableNoExistList.stream().map(item->item.get("tableName")).toArray(Object[]::new));
        map.put("noKeyTables", tableNoKeyList.stream().map(item->item.get("tableName")).toArray(Object[]::new));
        int allCount = readerTables.length;
        int errorCount = tableNoExistList.size() + tableNoKeyList.size();
        map.put("allCount", allCount);
        map.put("errorCount", errorCount);
        map.put("successCount", allCount-errorCount);
        return new ReturnT<>(map);
    }

    @Override
    public ReturnT<String> batchDeleteTask(String taskName, Integer[] projectIds) {
        //将需要删除的数据查出
        List<JobKafkaSync> jobKafkaSyncList = kafkaSyncMapper.getTaskList(taskName, projectIds);
        for (JobKafkaSync jobKafkaSync:jobKafkaSyncList) {
            executeCurl(JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors/", JSONObject.parseObject(jobKafkaSync.getReaderJson()).getString("name"), "DELETE");
            executeCurl(JobAdminConfig.getAdminConfig().getConnectorHost()+"connectors/", JSONObject.parseObject(jobKafkaSync.getWriterJson()).getString("name"), "DELETE");
        }
        kafkaSyncMapper.batchDeleteTask(taskName, projectIds);
        return ReturnT.SUCCESS;
    }

    /**
     * 将同步任务保存到表
     * @param syncJsonBuildDto
     * @param datasource
     * @return
     */
    private int saveKafkaSyncTask(SyncJsonBuildDto syncJsonBuildDto, JobDatasource datasource){
        //构建json
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("readerDatasourceId", syncJsonBuildDto.getReaderDatasourceId());
        jsonMap.put("readerDatasource", syncJsonBuildDto.getReaderDatasource());
        jsonMap.put("readerTableSchema", syncJsonBuildDto.getReaderTableSchema());
        jsonMap.put("readerTableName", syncJsonBuildDto.getReaderTableName());
        jsonMap.put("reServerName", syncJsonBuildDto.getReServerName());
        jsonMap.put("reServerId", syncJsonBuildDto.getReServerId());
        jsonMap.put("oracleVersion", syncJsonBuildDto.getOracleVersion());
        jsonMap.put("rePdbName", syncJsonBuildDto.getRePdbName());
        jsonMap.put("reKeyColumns", syncJsonBuildDto.getReKeyColumns());
        jsonMap.put("writerDatasourceId", syncJsonBuildDto.getWriterDatasourceId());
        jsonMap.put("writerDatasource", syncJsonBuildDto.getWriterDatasource());
        jsonMap.put("writerTableSchema", syncJsonBuildDto.getWriterTableSchema());
        jsonMap.put("writerTableName", syncJsonBuildDto.getWriterTableName());
        JobKafkaSync kafkaSync = new JobKafkaSync();
        kafkaSync.setId(syncJsonBuildDto.getTaskId());
        String url = datasource.getJdbcUrl();
        String port = SubMetaUtil.getUrlIpPort(url).get("port");
        String databaseName = url.substring(url.indexOf(port+"/") + 5);
        if(syncJsonBuildDto.getReaderDatasource().equals("sqlserver")){
            databaseName = url.substring(url.indexOf("DatabaseName=") + 13);
        }
        String taskName = syncJsonBuildDto.getTaskName();
        if(taskName.equals("")){
            if(syncJsonBuildDto.getReaderDatasource().equals("mysql")) {
                taskName = syncJsonBuildDto.getReaderDatasource() + "." + databaseName + "." + syncJsonBuildDto.getReaderTableSchema() + "." + syncJsonBuildDto.getReaderTableName();
            }else{
                taskName = syncJsonBuildDto.getReaderDatasource() + "." + databaseName + "." + syncJsonBuildDto.getReaderTableName();
            }
        }
        kafkaSync.setSyncName(taskName);
        kafkaSync.setReaderJson(syncJsonBuildDto.getReaderJson());
        kafkaSync.setWriterJson(syncJsonBuildDto.getWriterJson());
        kafkaSync.setSyncJson(JSON.toJSONString(jsonMap));
        kafkaSync.setProjectId(syncJsonBuildDto.getProjectId());
        int taskId = 0;
        //新增/修改
        if(syncJsonBuildDto.getTaskId() <= 0){
            kafkaSyncMapper.save(kafkaSync);
            taskId = kafkaSync.getId();
        } else{
            kafkaSyncMapper.update(kafkaSync);
            taskId = kafkaSync.getId();
        }
        return taskId;
    }

    /**
     * Windows 下发 curl命令
     */
//    public String executeCurl(String apiUrl, String param, String method){
//        try {
//            param = param.replaceAll("\\s+", "");
//            String url = "";
//            if(method.equals("POST")){
//                // Linux (弃用，无法下发指令)
//                //url = "curl -X POST \""+apiUrl+ "\" -H \"Content-Type: application/json\" -d '" +param+"' ";
//                // Windows (在本机测试，可以用这个方法)
//                param = param.replaceAll("\"", "\\\\\"");
//                url = "curl -X POST \""+apiUrl+"\" -H \"Content-Type: application/json\" -d \""+param+"\" ";
//            } else if(method.equals("DELETE")){
//                url = "curl -s "+apiUrl+param+" -X DELETE ";
//            }
//            System.out.println("url:"+url);
//            Process process = Runtime.getRuntime().exec(url);
//            InputStream inputStream = process.getInputStream();
//            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
//            String line;
//            StringBuilder output = new StringBuilder();
//            while ((line = reader.readLine()) != null) {
//                output.append(line);
//            }
//            return output.toString();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return "";
//    }

    /**
     * Linux 下发 curl命令
     */
    public String executeCurl(String apiUrl, String param, String method){
        String[] cmds;
        if(method.equals("POST")){
            cmds = new String[]{"curl",
                    apiUrl, "-k", "-H",
                    "Content-Type: application/json",
                    "-X", "POST",
                    "-d", param};
        } else{
            cmds = new String[]{"curl",
                    "-s", apiUrl+param, "-X",
                    "DELETE"};
        }
        ProcessBuilder process = new ProcessBuilder(cmds);
        Process p;
        try {
            p = process.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder sb = new StringBuilder();
            String line = null;
            while ((line = reader.readLine()) != null) {
                sb.append(line);
                sb.append(System.getProperty("line.separator"));
            }
            return sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public String executeHttpURLConnectionToGet(String apiUrl){
        HttpURLConnection connection = null;
        InputStream in=null;
        BufferedReader reader=null;
        try{
            //构造一个URL对象
            URL url = new URL(apiUrl);
            //获取URLConnection对象
            connection= (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("Accept", "*/*");
            connection.setRequestProperty("Connection", "keep-alive");
            connection.connect();
            //getOutputStream会隐含的进行connect(即：如同调用上面的connect()方法，所以在开发中不调用connect()也可以)
            in = connection.getInputStream();
            //通过InputStreamReader将字节流转换成字符串，在通过BufferedReader将字符流转换成自带缓冲流
            reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            String line = null;
            //按行读取
            while ((line = reader.readLine()) != null) {
                sb.append(line);
            }
            String response= sb.toString();
            return response;
        }catch (Exception exception){
            exception.printStackTrace();
        }finally {
            if (connection != null) {
                connection.disconnect();
            }
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }

}
