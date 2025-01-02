package com.wugui.hmt.executor.service.command;

import com.wugui.hmt.core.biz.model.TriggerParam;
import com.wugui.hmt.core.enums.IncrementTypeEnum;
import com.wugui.hmt.core.log.JobLogger;
import com.wugui.hmt.core.util.Constants;
import com.wugui.hmt.core.util.DateUtil;
import com.wugui.hmt.executor.util.SystemUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.wugui.hmt.core.util.Constants.SPLIT_COMMA;
import static com.wugui.hmt.executor.service.jobhandler.DataXConstant.*;


/*-------------------------------------------------------------------------
 *
 * BuildCommand.java
 *  command build
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
 *    /hmt-executor/src/main/java/com/wugui/hmt/executor/service/command/BuildCommand.java
 *
 *-----------------------------------------------
 */
public class BuildCommand {

    /**
     * DataX command build
     *
     * @param tgParam
     * @param tmpFilePath
     * @param dataXPyPath
     * @return
     */
    public static String[] buildDataXExecutorCmd(TriggerParam tgParam, String tmpFilePath, String dataXPyPath) {
        // command process
        //"--loglevel=debug"
        List<String> cmdArr = new ArrayList<>();
        cmdArr.add("python");
        //cmdArr.add("D:\\python27\\python.exe");
        String dataXHomePath = SystemUtils.getDataXHomePath();
        if (StringUtils.isNotEmpty(dataXHomePath)) {
            dataXPyPath = dataXHomePath.contains("bin") ? dataXHomePath + DEFAULT_DATAX_PY : dataXHomePath + "bin" + File.separator + DEFAULT_DATAX_PY;
        }
        cmdArr.add(dataXPyPath);
        String doc = buildDataXParam(tgParam);
        if (StringUtils.isNotBlank(doc)) {
            cmdArr.add(doc.replaceAll(SPLIT_SPACE, TRANSFORM_SPLIT_SPACE));
        }
        cmdArr.add(tmpFilePath);
        //datax.py里没有使用arg[1],后面的自定义参数
        //cmdArr.add(buildDataXCustomParam(tgParam));
        return cmdArr.toArray(new String[cmdArr.size()]);
    }

    private static String buildDataXCustomParam(TriggerParam tgParam) {
        StringBuilder doc = new StringBuilder();
        String customParam = StringUtils.isNotBlank(tgParam.getCustomParam()) ? tgParam.getCustomParam().trim() : tgParam.getCustomParam();
        if (StringUtils.isNotBlank(customParam)) {
            doc.append(PARAMS_CM).append(SPLIT_SPACE).append(customParam);
        }
        return doc.toString();
    }

    /**
     * 构建datax运行虚拟机参数
     *
     * @param tgParam
     * @return {@link String}
     * @author Locki
     * @date 2020/9/18
     */
    private static String buildDataXParam(TriggerParam tgParam) {
        StringBuilder doc = new StringBuilder();
        String jvmParam = StringUtils.isNotBlank(tgParam.getJvmParam()) ? tgParam.getJvmParam().trim() : tgParam.getJvmParam();
        if (StringUtils.isNotBlank(jvmParam)) {
            doc.append(JVM_CM).append(TRANSFORM_QUOTES).append(jvmParam).append(TRANSFORM_QUOTES);
        }
        return doc.toString();
    }

    /**
     * 构建datax增量参数
     *
     * @param tgParam
     * @return {@link HashMap< String, String>}
     * @author Locki
     * @date 2020/9/18
     */
    public static HashMap<String, String> buildDataXParamToMap(TriggerParam tgParam) {
        String partitionStr = tgParam.getPartitionInfo();
        Integer incrementType = tgParam.getIncrementType();
        String replaceParam = StringUtils.isNotBlank(tgParam.getReplaceParam()) ? tgParam.getReplaceParam().trim() : null;
        if (incrementType != null && replaceParam != null) {
            if (IncrementTypeEnum.ID.getCode() == incrementType) {
                long startId = tgParam.getStartId();
                long endId = tgParam.getEndId();
                String formatParam = String.format(replaceParam, startId, endId);
                return getKeyValue(formatParam);
            } else if (IncrementTypeEnum.TIME.getCode() == incrementType) {
                String replaceParamType = tgParam.getReplaceParamType();
                if (StringUtils.isBlank(replaceParamType) || "Timestamp".equals(replaceParamType)) {
                    long startTime = tgParam.getStartTime().getTime() / 1000;
                    long endTime = tgParam.getTriggerTime().getTime() / 1000;
                    String formatParam = String.format(replaceParam, startTime, endTime);
                    return getKeyValue(formatParam);
                } else {
                    SimpleDateFormat sdf = new SimpleDateFormat(replaceParamType);
                    String endTime = sdf.format(tgParam.getTriggerTime());
                    String startTime = sdf.format(tgParam.getStartTime());

                    String formatParam = String.format(replaceParam, startTime, endTime);
                    return getKeyValue(formatParam);
                }
            }
        }

        if (incrementType != null && IncrementTypeEnum.PARTITION.getCode() == incrementType) {
            if (StringUtils.isNotBlank(partitionStr)) {
                List<String> partitionInfo = Arrays.asList(partitionStr.split(SPLIT_COMMA));
                String formatParam = String.format(PARAMS_CM_V_PT, buildPartition(partitionInfo));
                return getKeyValue(formatParam);
            }

        }
        return null;
    }

    /**
     * 任务参数封装为map
     *
     * @param formatParam
     * @return {@link HashMap< String, String>}
     * @author Locki
     * @date 2020/9/18
     */
    private static HashMap<String, String> getKeyValue(String formatParam) {
        String[] paramArr = formatParam.split(PARAMS_SYSTEM);
        HashMap<String, String> map = new HashMap<String, String>();

        for (String param : paramArr) {
            if (StringUtils.isNotBlank(param)) {
                param = param.trim();
                String[] keyValue = param.split(PARAMS_EQUALS);
                map.put(keyValue[0], keyValue[1]);
            }
        }
        return map;
    }

    /**
     * datax任务内置变量：模仿阿里云商用DataWorks/ODPS提供内置变量<br/>
     * ${datax_bizdate}
     * ${datax_biztime}
     * ${datax_biz_unixtimestamp}
     *
     * @param
     * @return {@link Map< String, String>}
     * @author Locki
     * @date 2020/9/18
     */
    public static Map<String, String> builtInVar(){
        Map<String, String> map = new HashMap<String, String>();
        map.put("datax_biz_date", DateUtil.format(new Date(), "yyyy-MM-dd"));
        map.put("datax_biz_time", DateUtil.format(new Date(), "yyyy-MM-dd HH:mm:ss"));
        map.put("datax_biz_unixtimestamp", System.currentTimeMillis() + "");
        return map;
    }

    private void buildPartitionCM(StringBuilder doc, String partitionStr) {
        if (StringUtils.isNotBlank(partitionStr)) {
            doc.append(SPLIT_SPACE);
            List<String> partitionInfo = Arrays.asList(partitionStr.split(SPLIT_COMMA));
            doc.append(String.format(PARAMS_CM_V_PT, buildPartition(partitionInfo)));
        }
    }

    private static String buildPartition(List<String> partitionInfo) {
        String field = partitionInfo.get(0);
        int timeOffset = Integer.parseInt(partitionInfo.get(1));
        String timeFormat = partitionInfo.get(2);
        String partitionTime = DateUtil.format(DateUtil.addDays(new Date(), timeOffset), timeFormat);
        return field + Constants.EQUAL + partitionTime;
    }

}
