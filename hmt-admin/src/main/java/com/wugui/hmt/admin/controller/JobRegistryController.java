package com.wugui.hmt.admin.controller;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.metadata.IPage;
import com.baomidou.mybatisplus.extension.api.R;
import com.wugui.hmt.admin.entity.JobRegistry;
import com.wugui.hmt.admin.service.JobRegistryService;
import com.wugui.hmt.admin.util.PageUtils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;


/*-------------------------------------------------------------------------
 *
 * JobRegistryController.java
 *   执行器资源监控
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
 *	  hmt-admin/src/main/java/com/wugui/hmt/admin/controller/JobRegistryController.java
 *
 *-------------------------------------------------------------------------
 */
@RestController
@RequestMapping("/api/jobRegistry")
@Api(tags = "执行器资源监控")
public class JobRegistryController extends BaseController {

	@Autowired
	private JobRegistryService jobRegistryService;

	/**
	 * 分页查询所有数据
	 *
	 * @return 所有数据
	 */
	@GetMapping
	@ApiOperation("分页查询所有数据")
	@ApiImplicitParams(
			{@ApiImplicitParam(paramType = "query", dataType = "String", name = "current", value = "当前页", defaultValue = "1", required = true),
					@ApiImplicitParam(paramType = "query", dataType = "String", name = "size", value = "一页大小", defaultValue = "10", required = true),
					@ApiImplicitParam(paramType = "query", dataType = "Boolean", name = "ifCount", value = "是否查询总数", defaultValue = "true"),
					@ApiImplicitParam(paramType = "query", dataType = "String", name = "ascs", value = "升序字段，多个用逗号分隔"),
					@ApiImplicitParam(paramType = "query", dataType = "String", name = "descs", value = "降序字段，多个用逗号分隔")
			})
	public R<IPage<JobRegistry>> selectAll() {
		BaseForm baseForm = new BaseForm();
		return success(this.jobRegistryService.page(baseForm.getPlusPagingQueryEntity(), pageQueryWrapperCustom(baseForm.getParameters())));
	}

	/**
	 * 自定义查询组装
	 *
	 * @param map
	 * @return
	 */
	protected QueryWrapper<JobRegistry> pageQueryWrapperCustom(Map<String, Object> map) {
		// mybatis plus 分页相关的参数
		Map<String, Object> pageHelperParams = PageUtils.filterPageParams(map);
		//过滤空值，分页查询相关的参数
		Map<String, Object> columnQueryMap = PageUtils.filterColumnQueryParams(map);

		QueryWrapper<JobRegistry> queryWrapper = new QueryWrapper<>();

		//排序 操作
		pageHelperParams.forEach((k, v) -> {
			switch (k) {
				case "ascs":
					queryWrapper.orderByAsc(StrUtil.toUnderlineCase(StrUtil.toString(v)));
					break;
				case "descs":
					queryWrapper.orderByDesc(StrUtil.toUnderlineCase(StrUtil.toString(v)));
					break;
			}
		});

		//遍历进行字段查询条件组装
		columnQueryMap.forEach((k, v) -> {
			switch (k) {
				case "datasourceName":
					queryWrapper.like(StrUtil.toUnderlineCase(k), v);
					break;
				default:
					queryWrapper.eq(StrUtil.toUnderlineCase(k), v);
			}
		});

		return queryWrapper;
	}
}
