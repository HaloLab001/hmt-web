package com.wugui.hmt.admin.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import java.util.Date;

/*-------------------------------------------------------------------------
 *
 * JobInfo.java
 *   任务信息 实体类
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
 *	  hmt-admin/src/main/java/com/wugui/hmt/admin/entity/JobInfo.java
 *
 *-------------------------------------------------------------------------
 */
@Data
public class JobInfo {

	@ApiModelProperty("主键ID")
	private int id;

	@ApiModelProperty("执行器主键ID")
	private int jobGroup;

	@ApiModelProperty("任务执行CRON表达式")
	private String jobCron;

	@ApiModelProperty("排序")
	private String jobDesc;

	private Date addTime;

	private Date updateTime;

	@ApiModelProperty("修改用户")
	private int userId;

	@ApiModelProperty("报警邮件")
	private String alarmEmail;

	@ApiModelProperty("执行器路由策略")
	private String executorRouteStrategy;

	@ApiModelProperty("执行器，任务Handler名称")
	private String executorHandler;

	@ApiModelProperty("执行器，任务参数")
	private String executorParam;

	@ApiModelProperty("阻塞处理策略")
	private String executorBlockStrategy;

	@ApiModelProperty("任务执行超时时间，单位秒")
	private int executorTimeout;

	@ApiModelProperty("失败重试次数")
	private int executorFailRetryCount;

	@ApiModelProperty("GLUE类型\t#com.wugui.hmt.core.glue.GlueTypeEnum")
	private String glueType;

	@ApiModelProperty("GLUE源代码")
	private String glueSource;

	@ApiModelProperty("GLUE备注")
	private String glueRemark;

	@ApiModelProperty("GLUE更新时间")
	private Date glueUpdatetime;

	@ApiModelProperty("子任务ID")
	private String childJobId;

	@ApiModelProperty("调度状态：0-停止，1-运行")
	private int triggerStatus;

	@ApiModelProperty("上次调度时间")
	private long triggerLastTime;

	@ApiModelProperty("下次调度时间")
	private long triggerNextTime;

	@ApiModelProperty("hmt运行json")
	private String jobJson;

	@ApiModelProperty("脚本动态参数")
	private String replaceParam;

	@ApiModelProperty("增量日期格式")
	private String replaceParamType;

	@ApiModelProperty("jvm参数")
	private String jvmParam;

	@ApiModelProperty("自定义参数")
	private String customParam;

	@ApiModelProperty("增量初始时间")
	private Date incStartTime;

	@ApiModelProperty("分区信息")
	private String partitionInfo;

	@ApiModelProperty("最近一次执行状态")
	private int lastHandleCode;

	@ApiModelProperty("所属项目Id")
	private int projectId;

	@ApiModelProperty("主键字段")
	private String primaryKey;

	@ApiModelProperty("增量初始id")
	private Long incStartId;

	@ApiModelProperty("增量方式")
	private int incrementType;

	@ApiModelProperty("hmt的读表")
	private String readerTable;

	@ApiModelProperty("数据源id")
	private int datasourceId;

	@TableField(exist=false)
	private String projectName;

	@TableField(exist=false)
	private String userName;

	@ApiModelProperty("包含大字段: 0否 1是")
	private int isBigType;

	@ApiModelProperty("脚本文件存放路径")
	private String filePath;

	private String readerSchema;

	private Long readerDatasourceId;

	private String readerDataSource;
}
