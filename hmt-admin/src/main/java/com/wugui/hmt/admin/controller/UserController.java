package com.wugui.hmt.admin.controller;

import cn.hutool.core.util.StrUtil;
import com.wugui.hmt.core.biz.model.ReturnT;
import com.wugui.hmt.admin.core.util.I18nUtil;
import com.wugui.hmt.admin.entity.JobUser;
import com.wugui.hmt.admin.mapper.JobUserMapper;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.wugui.hmt.core.biz.model.ReturnT.FAIL_CODE;

/*-------------------------------------------------------------------------
 *
 * UserController.java
 *   用户信息相关接口
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
 *	  hmt-admin/src/main/java/com/wugui/hmt/admin/controller/UserController.java
 *
 *-------------------------------------------------------------------------
 */
@RestController
@RequestMapping("/api/user")
@Api(tags = "用户信息接口")
public class UserController {

    @Resource
    private JobUserMapper jobUserMapper;

    @Resource
    private BCryptPasswordEncoder bCryptPasswordEncoder;


    @GetMapping("/pageList")
    @ApiOperation("用户列表")
    public ReturnT<Map<String, Object>> pageList(@RequestParam(required = false, defaultValue = "1") int current,
                                                 @RequestParam(required = false, defaultValue = "10") int size,
                                                 String username) {

        // page list
        List<JobUser> list = jobUserMapper.pageList((current - 1) * size, size, username);
        int recordsTotal = jobUserMapper.pageListCount((current - 1) * size, size, username);

        // package result
        Map<String, Object> maps = new HashMap<>();
        maps.put("recordsTotal", recordsTotal);        // 总记录数
        maps.put("recordsFiltered", recordsTotal);    // 过滤后的总记录数
        maps.put("data", list);                    // 分页列表
        return new ReturnT<>(maps);
    }

    @GetMapping("/list")
    @ApiOperation("用户列表")
    public ReturnT<List<JobUser>> list(String username) {

        // page list
        List<JobUser> list = jobUserMapper.findAll(username);
        return new ReturnT<>(list);
    }

    @GetMapping("/getUserById")
    @ApiOperation(value = "根据id获取用户")
    public ReturnT<JobUser> selectById(@RequestParam("userId") Integer userId) {
        return new ReturnT<>(jobUserMapper.getUserById(userId));
    }

    @PostMapping("/add")
    @ApiOperation("添加用户")
    public ReturnT<String> add(@RequestBody JobUser jobUser) {

        // valid username
        if (!StringUtils.hasText(jobUser.getUsername())) {
            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_please_input") + I18nUtil.getString("user_username"));
        }
        jobUser.setUsername(jobUser.getUsername().trim());
        if (!(jobUser.getUsername().length() >= 4 && jobUser.getUsername().length() <= 20)) {
            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_length_limit") + "[4-20]");
        }
        // valid password
        if (!StringUtils.hasText(jobUser.getPassword())) {
            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_please_input") + I18nUtil.getString("user_password"));
        }
        jobUser.setPassword(jobUser.getPassword().trim());
        if (!(jobUser.getPassword().length() >= 4 && jobUser.getPassword().length() <= 20)) {
            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_length_limit") + "[4-20]");
        }
        jobUser.setPassword(bCryptPasswordEncoder.encode(jobUser.getPassword()));


        // check repeat
        JobUser existUser = jobUserMapper.loadByUserName(jobUser.getUsername());
        if (existUser != null) {
            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("user_username_repeat"));
        }

        // write
        jobUserMapper.save(jobUser);
        return ReturnT.SUCCESS;
    }

    @PostMapping(value = "/update")
    @ApiOperation("更新用户信息")
    public ReturnT<String> update(@RequestBody JobUser jobUser) {
        if (StringUtils.hasText(jobUser.getPassword())) {
            String pwd = jobUser.getPassword().trim();
            if (StrUtil.isBlank(pwd)) {
                return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_no_blank") + "密码");
            }

            if (!(pwd.length() >= 4 && pwd.length() <= 20)) {
                return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_length_limit") + "[4-20]");
            }
            jobUser.setPassword(bCryptPasswordEncoder.encode(pwd));
        } else {
            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_no_blank") + "密码");
        }
        // write
        jobUserMapper.update(jobUser);
        return ReturnT.SUCCESS;
    }

    @RequestMapping(value = "/remove", method = RequestMethod.POST)
    @ApiOperation("删除用户")
    public ReturnT<String> remove(int id) {
        int result = jobUserMapper.delete(id);
        return result != 1 ? ReturnT.FAIL : ReturnT.SUCCESS;
    }

    @PostMapping(value = "/updatePwd")
    @ApiOperation("修改密码")
    public ReturnT<String> updatePwd(@RequestBody JobUser jobUser) {
        String password = jobUser.getPassword();
        if (password == null || password.trim().length() == 0) {
            return new ReturnT<>(ReturnT.FAIL.getCode(), "密码不可为空");
        }
        password = password.trim();
        if (!(password.length() >= 4 && password.length() <= 20)) {
            return new ReturnT<>(FAIL_CODE, I18nUtil.getString("system_length_limit") + "[4-20]");
        }
        // do write
        JobUser existUser = jobUserMapper.loadByUserName(jobUser.getUsername());
        existUser.setPassword(bCryptPasswordEncoder.encode(password));
        jobUserMapper.update(existUser);
        return ReturnT.SUCCESS;
    }

}
