package com.wugui.hmt.admin.controller;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.wugui.hmt.admin.util.PageUtils;
import com.wugui.hmt.admin.util.ServletUtils;

import lombok.extern.slf4j.Slf4j;

import javax.servlet.http.HttpServletRequest;
import java.net.URLDecoder;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.Map;


/*-------------------------------------------------------------------------
 *
 * BaseForm.java
 *   基础参数辅助类
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
 *	  hmt-admin/src/main/java/com/wugui/hmt/admin/controller/BaseForm.java
 *
 *-------------------------------------------------------------------------
 */
@Slf4j
public class BaseForm {
    /**
     * 查询参数对象
     */
    protected Map<String, Object> values = new LinkedHashMap<>();

    /**
     * 当前页码
     */
    private Long current = 1L;

    /**
     * 页大小
     */
    private Long size = 10L;

    /**
     * 构造方法
     */
    public BaseForm() {
        try {
            HttpServletRequest request = ServletUtils.getRequest();
            Enumeration<String> params = request.getParameterNames();
            while (params.hasMoreElements()) {
                String name = params.nextElement();
                String value = StrUtil.trim(request.getParameter(name));
                this.set(name, URLDecoder.decode(value, "UTF-8"));
            }
            this.parsePagingQueryParams();
        } catch (Exception e) {
            e.printStackTrace();
            log.error("BaseControlForm initialize parameters setting error：" + e);
        }
    }

    /**
     * 获取页码
     *
     * @return
     */
    public Long getPageNo() {
        String pageNum = StrUtil.toString(this.get("current"));
        if (!StrUtil.isEmpty(pageNum) && NumberUtil.isNumber(pageNum)) {
            this.current = Long.parseLong(pageNum);
        }
        return this.current;
    }

    /**
     * 获取页大小
     *
     * @return
     */
    public Long getPageSize() {
        String pageSize = StrUtil.toString(this.get("size"));

        if (StrUtil.isNotEmpty(pageSize) && NumberUtil.isNumber(pageSize) && !"null".equalsIgnoreCase(pageSize)) {
            this.size = Long.parseLong(pageSize);
        }
        return this.size;
    }

    /**
     * 获得参数信息对象
     *
     * @return
     */
    public Map<String, Object> getParameters() {
        return values;
    }

    /**
     * 根据key获取values中的值
     *
     * @param name
     * @return
     */
    public Object get(String name) {
        if (values == null) {
            values = new LinkedHashMap<>();
            return null;
        }
        return this.values.get(name);
    }

    /**
     * 根据key获取values中String类型值
     *
     * @param key
     * @return String
     */
    public String getString(String key) {
        return StrUtil.toString(get(key));
    }

    /**
     * 获取排序字段
     *
     * @return
     */
    public String getSort() {
        return StrUtil.toString(this.values.get("sort"));
    }

    /**
     * 获取排序
     *
     * @return
     */
    public String getOrder() {
        return StrUtil.toString(this.values.get("order"));
    }

    /**
     * 获取排序
     *
     * @return
     */
    public String getOrderby() {
        return StrUtil.toString(this.values.get("orderby"));
    }

    /**
     * 解析出mybatis plus分页查询参数
     */
    public Page getPlusPagingQueryEntity() {
        Page page = new Page();
        //如果无current，默认返回1000条数据
        page.setCurrent(this.getPageNo());
        page.setSize(this.getPageSize());
        if (ObjectUtil.isNotNull(this.get("ifCount"))) {
            page.setSearchCount(BooleanUtil.toBoolean(this.getString("ifCount")));
        } else {
            //默认给true
            page.setSearchCount(true);
        }
        return page;
    }

    /**
     * 解析分页排序参数（pageHelper）
     */
    public void parsePagingQueryParams() {
        // 排序字段解析
        String orderBy = StrUtil.toString(this.get("orderby")).trim();
        String sortName = StrUtil.toString(this.get("sort")).trim();
        String sortOrder = StrUtil.toString(this.get("order")).trim().toLowerCase();

        if (StrUtil.isEmpty(orderBy) && !StrUtil.isEmpty(sortName)) {
            if (!sortOrder.equals("asc") && !sortOrder.equals("desc")) {
                sortOrder = "asc";
            }
            this.set("orderby", sortName + " " + sortOrder);
        }
    }


    /**
     * 设置参数
     *
     * @param name  参数名称
     * @param value 参数值
     */
    public void set(String name, Object value) {
        if (ObjectUtil.isNotNull(value)) {
            this.values.put(name, value);
        }
    }

    /**
     * 移除参数
     *
     * @param name
     */
    public void remove(String name) {
        this.values.remove(name);
    }

    /**
     * 清除所有参数
     */
    public void clear() {
        if (values != null) {
            values.clear();
        }
    }


    /**
     * 自定义查询组装
     *
     * @param map
     * @return
     */
    protected QueryWrapper<?> pageQueryWrapperCustom(Map<String, Object> map, QueryWrapper<?> queryWrapper) {
        // mybatis plus 分页相关的参数
        Map<String, Object> pageParams = PageUtils.filterPageParams(map);
        //过滤空值，分页查询相关的参数
        Map<String, Object> colQueryMap = PageUtils.filterColumnQueryParams(map);
        //排序 操作
        pageParams.forEach((k, v) -> {
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
        colQueryMap.forEach((k, v) -> {
            switch (k) {
                case "pluginName":
                case "datasourceName":
                    queryWrapper.like(StrUtil.toUnderlineCase(k), v);
                    break;
                case "type":
                    //目标端过滤dm数据源
                    queryWrapper.ne(v.equals("writer"), "datasource", "dm");
                    break;
                default:
                    queryWrapper.eq(StrUtil.toUnderlineCase(k), v);
            }
        });

        return queryWrapper;
    }

}