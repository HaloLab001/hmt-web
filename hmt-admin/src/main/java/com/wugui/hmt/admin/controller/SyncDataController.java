package com.wugui.hmt.admin.controller;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.admin.dto.SyncJsonBuildDto;
import com.wugui.hmt.admin.entity.JobKafkaSync;
import com.wugui.hmt.admin.service.SyncDataService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;


/*-------------------------------------------------------------------------
 *
 * SyncDataController.java
 *   数据增量同步相关接口
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
 *	  hmt-admin/src/main/java/com/wugui/hmt/admin/controller/SyncDataController.java
 *
 *-------------------------------------------------------------------------
 */
@Api(tags = "同步数据接口")
@RestController
@RequestMapping("/api/syncData")
public class SyncDataController extends BaseController{

    @Autowired
    private SyncDataService syncDataService;

    @GetMapping("/getConnectors")
    @ApiOperation("查看连接器信息")
    public ReturnT<JSONArray> getConnectors(){
        return new ReturnT<>(syncDataService.getConnectors());
    }

    @GetMapping("/getConnectorsStatus")
    @ApiOperation("查看连接器状态")
    public ReturnT<JSONObject> getConnectorsStatus(String name){
        return new ReturnT<>(syncDataService.getConnectorsStatus(name));
    }

    @GetMapping("/getStatusApiAddress")
    @ApiOperation("查看连接器状态接口地址")
    public ReturnT<JSONObject> getStatusApiAddress(String name){
        return new ReturnT<>(syncDataService.getStatusApiAddress(name));
    }

    @PostMapping("/createJson")
    @ApiOperation("生成Json")
    public ReturnT<JSONObject> createJson(@RequestBody Map<String, String> map){
        return new ReturnT<>(syncDataService.createJson(map));
    }

    @PostMapping("/save")
    @ApiOperation("保存任务")
    public ReturnT<JSONObject> saveTask(@RequestBody SyncJsonBuildDto syncJsonBuildDto){
        return syncDataService.saveTask(syncJsonBuildDto);
    }

    @GetMapping("/pageList")
    @ApiOperation("同步任务列表")
    public ReturnT<Map<String, Object>> pageList(@RequestParam(required = false, defaultValue = "0") int current,
                                                 @RequestParam(required = false, defaultValue = "10") int size,
                                                 String taskName, Integer[] projectIds) {
        return new ReturnT<>(syncDataService.pageList((current-1)*size, size, taskName, projectIds));
    }

    @RequestMapping(value = "/getInfoById", method = RequestMethod.POST)
    @ApiOperation("根据id获取任务信息")
    public ReturnT<JobKafkaSync> getInfoById(int id) {
        return syncDataService.getInfoById(id);
    }

    @RequestMapping(value = "/remove", method = RequestMethod.POST)
    @ApiOperation("删除任务")
    public ReturnT<String> remove(int id) {
        int result = syncDataService.delete(id);
        return result != 1 ? ReturnT.FAIL : ReturnT.SUCCESS;
    }

    @PostMapping("/batchSave")
    @ApiOperation("批量保存任务")
    public ReturnT<Map<String, Object>> batchSaveTask(@RequestBody SyncJsonBuildDto syncJsonBuildDto){
        return syncDataService.batchSaveTask(syncJsonBuildDto);
    }

    @RequestMapping(value = "/batchDeleteTask",method = RequestMethod.POST)
    @ApiOperation("批量删除任务")
    public ReturnT<String> batchDeleteTask(String taskName, Integer[] projectIds) {
        return syncDataService.batchDeleteTask(taskName, projectIds);
    }

}
