package com.wugui.hmt.rpc.util.json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/*-------------------------------------------------------------------------
 *
 * BasicJsonReader.java
 * BasicJsonReader类
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
 *    /hmt-rpc/src/main/java/com/wugui/hmt/rpc/util/json/BasicJsonReader.java
 *
 *-----------------------------------------------
 */
public class BasicJsonReader {
    private static Logger logger = LoggerFactory.getLogger(BasicJsonwriter.class);


    public Map<String, Object> parseMap(String json) {
        if (json != null) {
            json = json.trim();
            if (json.startsWith("{")) {
                return parseMapInternal(json);
            }
        }
        throw new IllegalArgumentException("Cannot parse JSON");
    }

    public List<Object> parseList(String json) {
        if (json != null) {
            json = json.trim();
            if (json.startsWith("[")) {
                return parseListInternal(json);
            }
        }
        throw new IllegalArgumentException("Cannot parse JSON");
    }


    private List<Object> parseListInternal(String json) {
        List<Object> list = new ArrayList<Object>();
        json = trimLeadingCharacter(trimTrailingCharacter(json, ']'), '[');
        for (String value : tokenize(json)) {
            list.add(parseInternal(value));
        }
        return list;
    }

    private Object parseInternal(String json) {
        if (json.equals("null")) {
            return null;
        }
        if (json.startsWith("[")) {
            return parseListInternal(json);
        }
        if (json.startsWith("{")) {
            return parseMapInternal(json);
        }
        if (json.startsWith("\"")) {
            return trimTrailingCharacter(trimLeadingCharacter(json, '"'), '"');
        }
        try {
            return Long.valueOf(json);
        } catch (NumberFormatException ex) {
            // ignore
        }
        try {
            return Double.valueOf(json);
        } catch (NumberFormatException ex) {
            // ignore
        }
        return json;
    }

    private Map<String, Object> parseMapInternal(String json) {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        json = trimLeadingCharacter(trimTrailingCharacter(json, '}'), '{');
        for (String pair : tokenize(json)) {
            String[] values = trimArrayElements(split(pair, ":"));
            String key = trimLeadingCharacter(trimTrailingCharacter(values[0], '"'), '"');
            Object value = parseInternal(values[1]);
            map.put(key, value);
        }
        return map;
    }

    // append start
    private static String[] split(String toSplit, String delimiter) {
        if (toSplit != null && !toSplit.isEmpty() && delimiter != null && !delimiter.isEmpty()) {
            int offset = toSplit.indexOf(delimiter);
            if (offset < 0) {
                return null;
            } else {
                String beforeDelimiter = toSplit.substring(0, offset);
                String afterDelimiter = toSplit.substring(offset + delimiter.length());
                return new String[]{beforeDelimiter, afterDelimiter};
            }
        } else {
            return null;
        }
    }

    private static String[] trimArrayElements(String[] array) {
        if (array == null || array.length == 0) {
            return new String[0];
        } else {
            String[] result = new String[array.length];

            for (int i = 0; i < array.length; ++i) {
                String element = array[i];
                result[i] = element != null ? element.trim() : null;
            }

            return result;
        }
    }


    private List<String> tokenize(String json) {
        List<String> list = new ArrayList<>();
        int index = 0;
        int inObject = 0;
        int inList = 0;
        boolean inValue = false;
        boolean inEscape = false;
        StringBuilder build = new StringBuilder();
        while (index < json.length()) {
            char current = json.charAt(index);
            if (inEscape) {
                build.append(current);
                index++;
                inEscape = false;
                continue;
            }
            if (current == '{') {
                inObject++;
            }
            if (current == '}') {
                inObject--;
            }
            if (current == '[') {
                inList++;
            }
            if (current == ']') {
                inList--;
            }
            if (current == '"') {
                inValue = !inValue;
            }
            if (current == ',' && inObject == 0 && inList == 0 && !inValue) {
                list.add(build.toString());
                build.setLength(0);
            } else if (current == '\\') {
                inEscape = true;
            } else {
                build.append(current);
            }
            index++;
        }
        if (build.length() > 0) {
            list.add(build.toString());
        }
        return list;
    }

    // plugin util
    private static String trimTrailingCharacter(String string, char c) {
        if (string.length() > 0 && string.charAt(string.length() - 1) == c) {
            return string.substring(0, string.length() - 1);
        }
        return string;
    }

    private static String trimLeadingCharacter(String string, char c) {
        if (string.length() > 0 && string.charAt(0) == c) {
            return string.substring(1);
        }
        return string;
    }

}
