package com.wugui.hmt.admin.entity;

import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.Date;


/*-------------------------------------------------------------------------
 *
 * JobLog.java
 *   log, used to track trigger process
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
 * This software and related documentation are provided under a 
 * license agreement containing restrictions on use and disclosure
 * and are protected by intellectual property laws. Except as expressly
 * permitted in your license agreement or allowed by law, you may not 
 * use, copy, reproduce, translate, broadcast, modify, license, transmit,
 * distribute, exhibit, perform, publish, or display any part, in any 
 * form, or by any means. Reverse engineering, disassembly, or 
 * decompilation of this software, unless required by law for 
 * interoperability, is prohibited.
 * 
 * This software is developed for general use in a variety of
 * information management applications. It is not developed or intended
 * for use in any inherently dangerous applications, including 
 * applications that may create a risk of personal injury. If you use 
 * this software or in dangerous applications, then you shall be 
 * responsible to take all appropriate fail-safe, backup, redundancy,
 * and other measures to ensure its safe use. Halo Tech Corporation and
 * its affiliates disclaim any liability for any damages caused by use
 * of this software in dangerous applications.
 * 
 *
 * IDENTIFICATION
 *	  hmt-admin/src/main/java/com/wugui/hmt/admin/entity/JobLog.java
 *
 *-------------------------------------------------------------------------
 */
@Data
public class JobLog {

    private long id;

    // job info
    @ApiModelProperty("执行器主键ID")
    private int jobGroup;
    @ApiModelProperty("任务，主键ID")
    private int jobId;
    @ApiModelProperty("任务描述")
    private String jobDesc;

    // execute info
    @ApiModelProperty("执行器地址，本次执行的地址")
    private String executorAddress;
    @ApiModelProperty("执行器任务handler")
    private String executorHandler;
    @ApiModelProperty("执行器任务参数")
    private String executorParam;
    @ApiModelProperty("执行器任务分片参数，格式如 1/2")
    private String executorShardingParam;
    @ApiModelProperty("失败重试次数")
    private int executorFailRetryCount;

    // trigger info
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    @ApiModelProperty("调度-时间")
    private Date triggerTime;
    @ApiModelProperty("调度-结果")
    private int triggerCode;
    @ApiModelProperty("调度-日志")
    private String triggerMsg;

    // handle info
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    @ApiModelProperty("执行-时间")
    private Date handleTime;
    @ApiModelProperty("执行-状态")
    private int handleCode;
    @ApiModelProperty("执行-日志")
    private String handleMsg;

    // alarm info
    @ApiModelProperty("告警状态：0-默认、1-无需告警、2-告警成功、3-告警失败")
    private int alarmStatus;

    @ApiModelProperty("hmt进程Id")
    private String processId;

    @ApiModelProperty("增量最大id")
    private Long maxId;

    @ApiModelProperty("任务执行CRON(现作为元数据类型去使用)")
    private String jobCron;
}
