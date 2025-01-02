package com.wugui.hmt.admin.core.newScheduler;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.admin.dto.DataXBatchJsonBuildDto;
import com.wugui.hmt.admin.dto.RdbmsReaderDto;
import com.wugui.hmt.admin.dto.RdbmsWriterDto;
import com.wugui.hmt.admin.entity.JobDatasource;
import com.wugui.hmt.admin.entity.JobGroup;
import com.wugui.hmt.admin.entity.JobProject;
import com.wugui.hmt.admin.entity.JobTemplate;
import com.wugui.hmt.admin.mapper.JobGroupMapper;
import com.wugui.hmt.admin.mapper.JobTemplateMapper;
import com.wugui.hmt.admin.service.DatasourceQueryService;
import com.wugui.hmt.admin.service.JobDatasourceService;
import com.wugui.hmt.admin.service.JobProjectService;
import com.wugui.hmt.admin.service.JobService;
import com.wugui.hmt.admin.service.SubMetaDataService;
import com.wugui.hmt.admin.tool.pojo.MetaTypePojo;
import com.wugui.hmt.admin.tool.pojo.SubCheckTablePojo;
import com.wugui.hmt.admin.util.SubMetaTypeEnum;
import com.wugui.hmt.admin.util.SubMetaUtil;

/*-------------------------------------------------------------------------
 *
 * AutomationSchedule.java
 *  AutomationSchedule类
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/core/newScheduler/AutomationSchedule.java
 *
 *-----------------------------------------------
 */
@Component
public class AutomationSchedule {

    private static final Logger logger = Logger.getLogger(AutomationSchedule.class.getName());
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    @Autowired
    private JobProjectService jobProjectService;

    @Autowired
    private JobService jobService;

    @Autowired
    private JobGroupMapper jobGroupMapper;

    @Autowired
    private DatasourceQueryService datasourceQueryService;
    
    @Autowired
    private JobDatasourceService jobJdbcDatasourceService;

    @Autowired
    private JobTemplateMapper jobTemplateMapper;

    @Autowired
    private SubMetaDataService subMetaDataService;

    @Scheduled(fixedRate = 1000)
    public void executeTrigger() {
        // 捕获需要执行的自动化迁移任务
        List<JobProject> jobProjectList = jobProjectService.lambdaQuery().eq(JobProject::getTriggerStatus, 1).eq(JobProject::getRunningStatus, 0).list();
        if(!jobProjectList.isEmpty()){
            logger.info("捕获到了"+jobProjectList.size()+"个需要执行的自动化迁移任务");
            for(int i = 0; i < jobProjectList.size(); i++){
                // 记录开始时间（纳秒）
                long startTime = System.nanoTime();
                int taskIndex = i+1;
                int taskId = jobProjectList.get(i).getId();
                logger.info("开始执行第"+taskIndex+"个自动化迁移任务，同时开始写入日志");
                // 配置Logger输出到文件
                FileHandler fileHandler;
                try {
                    // 日志存放路径
                    String loggerFilePath = getFilePath(taskId);
                    // 先将该任务的调度状态变更为运行中
                    jobProjectService.lambdaUpdate().set(JobProject::getRunningStatus, 1).set(JobProject::getLoggerFilePath, loggerFilePath).eq(JobProject::getId, taskId).update();
                    // 写入日志信息
                    fileHandler = new FileHandler(loggerFilePath, true);
                    logger.addHandler(fileHandler);
                    //SimpleFormatter formatter = new SimpleFormatter();
                    // 使用自定义Formatter
                    CustomFormatter formatter = new CustomFormatter();
                    fileHandler.setFormatter(formatter);
                
                    /*
                    * 任务调度区开始
                    */
                    logger.info("开始运行自动化迁移任务......");

                    // 将数据源信息查询出,便于后续引用
                    JobDatasource readerDatasources = jobJdbcDatasourceService.getById(jobProjectList.get(i).getReaderDatasourceId());
                    JobDatasource writerDatasources = jobJdbcDatasourceService.getById(jobProjectList.get(i).getWriterDatasourceId());

                    // 检查执行器服务是否正常
                    JobGroup jobGroup = jobGroupMapper.load(1);
                    if(jobGroup.getAddressList() == null || jobGroup.getAddressList().equals("")){
                        //logger.severe("执行器服务地址为空，请先检测执行器服务是否正常！");
                        throw new RuntimeException("执行器服务地址为空，请先检查执行器服务是否正常！");
                    }

                    // 检查目标端的schema是否存在
                    String writerSchema = jobProjectList.get(i).getReaderSchema().toLowerCase();
                    logger.info("正在检查目标端的schema是否存在......");
                    logger.info("schema名称: "+writerSchema);
                    boolean checkSchema = datasourceQueryService.checkSchemaSql(writerDatasources, writerSchema);
                    logger.info("schema检查结果: "+checkSchema);
                    if(checkSchema){
                        logger.info("schema已经存在！");
                    } else{
                        logger.info("schema不存在，正在创建中......");
                        boolean createSchema = datasourceQueryService.createSchemaSQL(writerDatasources, writerSchema);
                        if(createSchema){
                            logger.info("schema创建成功！");
                        } else{
                            throw new RuntimeException("schema创建失败！");
                        }
                    }

                    // 获取元数据类型
                    List<MetaTypePojo> metaTypePojoList = getMetaTypePojoList(readerDatasources.getDatasource());
                    for (MetaTypePojo metaTypePojo : metaTypePojoList) {
                        // 元数据类型信息
                        String metaTypeId = String.valueOf(metaTypePojo.getMetaTypeId());
                        String metaTypeName = metaTypePojo.getMetaTypeName();
                        logger.info("-------------------------------------------------------------");
                        logger.info("正在检查"+metaTypeName+"迁移任务......");
                        // 处理数据迁移任务
                        if(metaTypePojo.getMetaTypeId() == -1){
                            executeTriggerDataMigration(jobProjectList.get(i), readerDatasources);
                            metaTypeId = null;
                        }
                        // 获取当前任务的数量
                        int metaAwaitCount = jobService.getJobCount(taskId, metaTypeId, 0, new Integer[]{0});
                        logger.info("发现("+metaAwaitCount+")个需要迁移的"+metaTypeName+"任务");
                        if(metaAwaitCount > 0){
                            logger.info("正在批量启动"+metaTypeName+"迁移任务......");
                            // 批量启动当前迁移任务
                            jobService.automationBatchStart(taskId, metaTypeId);
                            logger.info("已成功批量启动"+metaTypeName+"迁移任务，等待迁移完成中......");
                            logger.info("正在启动监听"+metaTypeName+"任务......");
                            // 控制失败的重复日志打印
                            int isFail = 0;
                            // 控制只执行一次失败任务切换数据插入方式
                            int isInert = 0;
                            // 监听迁移任务是否完成
                            while (true) {
                                try {
                                    // 此处暂时延迟3秒,需要等待执行器完成任务,没必要查询的太快
                                    Thread.sleep(3000);
                                } catch (InterruptedException e) {
                                    //logger.info(e.getMessage());
                                    throw new RuntimeException(e.getMessage());
                                }
                                // 待执行的数量
                                int newAwaitCount = jobService.getJobCount(taskId, metaTypeId, 1, new Integer[]{0});
                                // 失败的数量
                                int newFailCount = jobService.getJobCount(taskId, metaTypeId, 1, new Integer[]{500,502});
                                // 执行中的数量
                                int newRunningCount = jobService.getJobCount(taskId, metaTypeId, 1, new Integer[]{1});
                                // 成功的数量
                                int newSuccessCount = jobService.getJobCount(taskId, metaTypeId, 1, new Integer[]{200});
                                // 监听结束条件
                                if(newAwaitCount == 0 && newRunningCount == 0 && newFailCount == 0){
                                    // set当前元数据迁移成功的总数,便于后续统计
                                    metaTypePojo.setSucTotal(newSuccessCount);
                                    logger.info("监听当前迁移最终结果: 成功迁移数量-("+newSuccessCount+")");
                                    break; 
                                } else if(newAwaitCount == 0 && newRunningCount == 0 && newFailCount > 0){
                                    // 不需要一直输出等待日志
                                    if(isFail == 0){
                                        // 等待前再输出一遍监听统计结果即可
                                        logger.info("监听当前迁移任务结果: 待执行-("+newAwaitCount+")  失败的-("+newFailCount+")  执行中-("+newRunningCount+")  成功的-("+newSuccessCount+")");
                                        // 对数据迁移失败的任务再进行处理
                                        if(metaTypeId == null && isInert == 0){
                                            logger.info("正在进入失败任务重试阶段......");
                                            logger.info("正在将失败的任务切换成inert数据插入方式......");
                                            // 将失败的任务转换成insert插入方式,并再次执行
                                            jobService.automationBatchSetDataWay(taskId, 1, "500", "halowriter");
                                            logger.info("halowriter切换成功，等待任务重新执行，继续监听迁移结果中......");
                                            isInert = 1;
                                            // 跳出,继续监听迁移结果
                                            continue;
                                        }
                                        // 导致任务阻塞,使用severe告知
                                        logger.severe("需要手动将失败的"+metaTypeName+"迁移任务进行处理，正在等待中......");
                                    }
                                    isFail = newFailCount;
                                } else{
                                    logger.info("监听当前迁移任务结果: 待执行-("+newAwaitCount+")  失败的-("+newFailCount+")  执行中-("+newRunningCount+")  成功的-("+newSuccessCount+")");
                                }
                            }
                            logger.info("成功结束监听"+metaTypeName+"任务......");
                        } else{
                            logger.info("没有需要迁移的"+metaTypeName+"任务");
                        }
                        logger.info(metaTypeName+"迁移任务执行完毕");
                        logger.info("-------------------------------------------------------------");
                    }

                    /*
                    * 任务调度区结束
                    */

                    logger.info("本次自动化迁移任务总体运行情况如下");
                    // 输出最终迁移统计结果
                    for (MetaTypePojo metaTypePojo : metaTypePojoList) {
                        //String metaTypeId = String.valueOf(metaTypePojo.getMetaTypeId());
                        String metaTypeName = metaTypePojo.getMetaTypeName();
                        int sucTotal = metaTypePojo.getSucTotal();
                        logger.info(metaTypeName+": "+sucTotal);
                    }
                    // 记录结束时间（纳秒）
                    long endTime = System.nanoTime();
                    // 计算耗时（纳秒）
                    long duration = endTime - startTime;
                    // 将耗时转换为秒（1秒 = 1,000,000,000纳秒）
                    double durationInSeconds = duration / 1_000_000_000.0;
                    logger.info("该自动化迁移任务总计耗时: "+String.format("%.2f", durationInSeconds)+"/秒");

                    // 任务结束,将该任务的调度状态变更为已完成
                    jobProjectService.lambdaUpdate().set(JobProject::getTriggerStatus, 0).set(JobProject::getRunningStatus, 3).eq(JobProject::getId, taskId).update();
                    logger.info("自动化迁移任务结束，状态已变更为已完成");
                } catch (Exception e) {
                    logger.severe("自动化迁移过程中发生异常: " + e.getMessage());
                    // 任务异常终止,变更任务状态
                    jobProjectService.lambdaUpdate().set(JobProject::getTriggerStatus, 0).set(JobProject::getRunningStatus, 2).eq(JobProject::getId, taskId).update();
                } finally{
                    // 注意: 通常不建议在这里关闭处理器，因为这可能会导致日志消息丢失.如果确实需要关闭，请确保所有日志消息都已写入。
                    // 关闭Logger
                    for (java.util.logging.Handler handler : logger.getHandlers()) {
                        handler.close();
                    }
                }
            }
        }
    }
    

    /**
     * 对数据迁移做自动化
     * @param jobProject
     */
    public void executeTriggerDataMigration(JobProject jobProject, JobDatasource readerDatasources){
        Long readerDatasourceId = jobProject.getReaderDatasourceId();
        Long writerDatasourceId = jobProject.getWriterDatasourceId();
        String readerSchema = jobProject.getReaderSchema();
        try {
            // 先检查一遍数据迁移任务总数量
            int taskCount = jobService.getJobCount(jobProject.getId(), null, 1, new Integer[]{0,1,200,500,502});
            if(taskCount > 0) {
                logger.info("发现该数据迁移任务已经生成过，总任务数据量为: "+taskCount+" ，直接跳过创建任务！");
                return;
            }
            logger.info("正在根据任务模板开始准备生成数据迁移任务......");
            JobTemplate jobTemplate = jobTemplateMapper.loadByProjectId(jobProject.getId());
            // 获取源端与目标端的表信息
            List<String> readerTableList = datasourceQueryService.getTables(readerDatasourceId, readerSchema);
            List<String> writerTableList = datasourceQueryService.getTables(writerDatasourceId, readerSchema.toLowerCase());
            // 检查源端和目标端的表信息是否一致
            logger.info("正在检查源端和目标端的表信息是否一致......");
            SubCheckTablePojo subCheckTablePojo = new SubCheckTablePojo();
            subCheckTablePojo.setReaderSchema(readerSchema);
            subCheckTablePojo.setReaderTables(readerTableList);
            subCheckTablePojo.setWriterSchema(readerSchema.toLowerCase());
            subCheckTablePojo.setWriterTables(writerTableList);
            Map<String, Object> subCheckMap = subMetaDataService.checkTableDifference(subCheckTablePojo);
            logger.info("源端特有表: "+subCheckMap.get("readerTables"));
            logger.info("目标端特有表: "+subCheckMap.get("writerTables"));
            // 生成数据迁移任务的参数配置
            DataXBatchJsonBuildDto dataXBatchJsonBuildDto = new DataXBatchJsonBuildDto();
            dataXBatchJsonBuildDto.setReaderDatasourceId(readerDatasourceId);
            dataXBatchJsonBuildDto.setReaderDataSource(readerDatasources.getDatasource());
            dataXBatchJsonBuildDto.setReaderSchema(readerSchema);
            dataXBatchJsonBuildDto.setWriterDatasourceId(writerDatasourceId);
            dataXBatchJsonBuildDto.setWriteDataSource("halo");
            dataXBatchJsonBuildDto.setWriterSchema(readerSchema.toLowerCase());
            dataXBatchJsonBuildDto.setTemplateId(jobTemplate.getId());
            dataXBatchJsonBuildDto.setReaderTables(readerTableList);
            dataXBatchJsonBuildDto.setWriterTables(writerTableList);
            RdbmsReaderDto rdbmsReader = new RdbmsReaderDto();
            rdbmsReader.setReaderSplitPk("");
            dataXBatchJsonBuildDto.setRdbmsReader(rdbmsReader);
            RdbmsWriterDto rdbmsWriterDto = new RdbmsWriterDto();
            rdbmsWriterDto.setPostSql("");
            rdbmsWriterDto.setPreSql("");
            dataXBatchJsonBuildDto.setRdbmsWriter(rdbmsWriterDto);
            logger.info("正在批量生成数据迁移任务......");
            // 生成数据迁移任务
			ReturnT<String> jReturnT = jobService.batchAdd(dataXBatchJsonBuildDto);
            if(jReturnT.getCode() == ReturnT.FAIL_CODE){
                throw new RuntimeException("批量生成数据迁移任务失败: " + jReturnT.getMsg());
            }
            logger.info("数据迁移任务成功创建");
		} catch (Exception e) {
            throw new RuntimeException("批量生成数据迁移任务失败: " + e);
		}
    }

    /**
     * 控制需要迁移的任务
     * @param datasource
     * @return
     */
    public List<MetaTypePojo> getMetaTypePojoList(String datasource){
        List<MetaTypePojo> metaTypePojoList = new ArrayList<>();
        for (SubMetaTypeEnum subMetaEnum : EnumSet.allOf(SubMetaTypeEnum.class)) {
            MetaTypePojo metaTypePojo = new MetaTypePojo();
            metaTypePojo.setMetaTypeId(subMetaEnum.getMetaTypeId());
            metaTypePojo.setMetaTypeName(subMetaEnum.getMetaTypeName());
            metaTypePojo.setObjectType(subMetaEnum.getObjectType());
            metaTypePojoList.add(metaTypePojo);
        }
        // 加入数据迁移
        MetaTypePojo nMetaTypePojo = new MetaTypePojo();
        nMetaTypePojo.setMetaTypeId(-1);
        nMetaTypePojo.setMetaTypeName("数据迁移(表个数)");
        metaTypePojoList.add(nMetaTypePojo);
        // 控制顺序
        String sortStringArray = "";
        if(datasource.equals("oracle")){
            sortStringArray = "2,-1,10,1,5,6,9,7,12,8,3,4,11";
        } else if(datasource.equals("mysql")){
            sortStringArray = "2,-1,10,5,6,7,8,3,4,11";
        }
        String[] stringArray = sortStringArray.split(",");
        // 用于存储按自定义顺序排列的对象的新集合
        List<MetaTypePojo> sortedList = new ArrayList<>();
        // 循环自定义ID顺序，并从原始集合中查找对应的对象
        for (String id : stringArray) {
            for (MetaTypePojo pojo : metaTypePojoList) {
                if (pojo.getMetaTypeId() == Integer.parseInt(id)) {
                    sortedList.add(pojo);
                    break;
                }
            }
        }
        return sortedList;
    }

    /**
     * 获取日志文件存放路径
     * @param taskId
     * @return
     */
    public String getFilePath(int taskId){
        String filePath = this.getClass().getResource("/").getPath()+"meta/log/automationLog";
        // 创建File对象
        File directory = new File(filePath);
        // 如果文件夹不存在，则创建文件夹
        if (!directory.exists()) {
            directory.mkdirs();
        }
        SimpleDateFormat dateFormat= new SimpleDateFormat("yyyyMMddhhmmss");
        filePath = filePath + "/" + taskId + "-" + dateFormat.format(new Date()) + ".log";
        return filePath;
    }

    /**
     * 自定义日志输出内容 Formatter类
     */
    private static class CustomFormatter extends Formatter {
        @Override
        public String format(LogRecord record) {
            Date date = new Date(record.getMillis());
            String timestamp = dateFormat.format(date);
            StringBuilder sb = new StringBuilder();
            // 由于原日志前缀太长,所以使用自定义的输出 -> 当前时间 当前方法名 日志等级 日志输出内容
            sb.append(timestamp).append(" ").append("Halo executeTrigger ").append(record.getLevel()).append(": ").append(record.getMessage()).append(System.lineSeparator());
            return sb.toString();
        }
    }

}
