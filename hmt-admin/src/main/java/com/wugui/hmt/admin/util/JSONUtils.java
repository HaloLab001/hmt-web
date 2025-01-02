package com.wugui.hmt.admin.util;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


/*-------------------------------------------------------------------------
 *
 * JSONUtils.java
 *  DataX JSON 用户名密码解密
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/util/JSONUtils.java
 *
 *-----------------------------------------------
 */
public class JSONUtils {

    /**
     * decrypt 解密
     */
    public static Integer decrypt = 0;
    /**
     * decrypt 加密
     */
    public static Integer encrypt = 1;

    /**
     * @param content
     * @param key
     * @param changeType 0加密 or 1解密
     * @return
     */
    public static JSONObject change(String content, String key, Integer changeType) {
        JSONObject keyObj = JSONObject.parseObject(JSONObject.parseObject(content).getString(key));
        JSONObject params = JSONObject.parseObject(keyObj.getString("parameter"));
        String dUsername = null, dPassword = null;
        if (decrypt.equals(changeType)) { //解密
            dUsername = AESUtil.decrypt(params.getString("username"));
            dPassword = AESUtil.decrypt(params.getString("password"));

        } else if (encrypt.equals(changeType)) {//加密

            dUsername = AESUtil.encrypt(params.getString("username"));
            dPassword = AESUtil.encrypt(params.getString("password"));
        }
        String username = dUsername == null ? params.getString("username") : dUsername;
        String password = dPassword == null ? params.getString("password") : dPassword;
        params.put("username", username);
        params.put("password", password);
        keyObj.put("parameter", params);
        return keyObj;
    }

    /**
     * @param jsonStr
     * @param changeType 0加密 or 1解密
     * @return jsonStr
     */
    public static String changeJson(String jsonStr, Integer changeType) {
        JSONObject json = JSONObject.parseObject(jsonStr);
        JSONObject job = json.getJSONObject("job");
        JSONArray contents = job.getJSONArray("content");
        for (int i = 0; i < contents.size(); i++) {
            String contentStr = contents.getString(i);
            Object obj = contents.get(i);
            if (decrypt.equals(changeType)) { //解密
                ((JSONObject) obj).put("reader", change(contentStr, "reader", decrypt));
                ((JSONObject) obj).put("writer", change(contentStr, "writer", decrypt));
            } else if (encrypt.equals(changeType)) {//加密
                ((JSONObject) obj).put("reader", change(contentStr, "reader", encrypt));
                ((JSONObject) obj).put("writer", change(contentStr, "writer", encrypt));
            }
        }
        job.put("content", contents);
        json.put("job", job);
        return json.toJSONString();
    }
}
