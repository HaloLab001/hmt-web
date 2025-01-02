package com.wugui.hmt.rpc.util.json;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;


/*-------------------------------------------------------------------------
 *
 * BasicJsonwriter.java
 * BasicJsonwriter类
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
 *    /hmt-rpc/src/main/java/com/wugui/hmt/rpc/util/json/BasicJsonwriter.java
 *
 *-----------------------------------------------
 */
public class BasicJsonwriter {
    private static Logger logger = LoggerFactory.getLogger(BasicJsonwriter.class);


    private static final String STR_SLASH = "\"";
    private static final String STR_SLASH_STR = "\":";
    private static final String STR_COMMA = ",";
    private static final String STR_OBJECT_LEFT = "{";
    private static final String STR_OBJECT_RIGHT = "}";
    private static final String STR_ARRAY_LEFT = "[";
    private static final String STR_ARRAY_RIGHT = "]";

    private static final Map<String, Field[]> cacheFields = new HashMap<>();

    /**
     * write object to json
     *
     * @param object
     * @return
     */
    public String toJson(Object object) {
        StringBuilder json = new StringBuilder();
        try {
            writeObjItem(null, object, json);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        // replace
        String str = json.toString();
        if (str.contains("\n")) {
            str = str.replaceAll("\\n", "\\\\n");
        }
        if (str.contains("\t")) {
            str = str.replaceAll("\\t", "\\\\t");
        }
        if (str.contains("\r")) {
            str = str.replaceAll("\\r", "\\\\r");
        }
        return str;
    }

    /**
     * append Obj
     *
     * @param key
     * @param value
     * @param json  "key":value or value
     */
    private void writeObjItem(String key, Object value, StringBuilder json) {

        /*if ("serialVersionUID".equals(key)
                || value instanceof Logger) {
            // pass

            return;
        }*/

        // "key:"
        if (key != null) {
            json.append(STR_SLASH).append(key).append(STR_SLASH_STR);
        }

        // val
        if (value == null) {
            json.append("null");
        } else if (value instanceof String
                || value instanceof Byte
                || value instanceof CharSequence) {
            // string

            json.append(STR_SLASH).append(value.toString()).append(STR_SLASH);
        } else if (value instanceof Boolean
                || value instanceof Short
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Float
                || value instanceof Double
        ) {
            // number

            json.append(value);
        } else if (value instanceof Object[] || value instanceof Collection) {
            // collection | array     //  Array.getLength(array);   // Array.get(array, i);

            Collection valueColl = null;
            if (value instanceof Object[]) {
                Object[] valueArr = (Object[]) value;
                valueColl = Arrays.asList(valueArr);
            } else if (value instanceof Collection) {
                valueColl = (Collection) value;
            }

            json.append(STR_ARRAY_LEFT);
            if (valueColl.size() > 0) {
                for (Object obj : valueColl) {
                    writeObjItem(null, obj, json);
                    json.append(STR_COMMA);
                }
                json.delete(json.length() - 1, json.length());
            }
            json.append(STR_ARRAY_RIGHT);

        } else if (value instanceof Map) {
            // map

            Map<?, ?> valueMap = (Map<?, ?>) value;

            json.append(STR_OBJECT_LEFT);
            if (!valueMap.isEmpty()) {
                Set<?> keys = valueMap.keySet();
                for (Object valueMapItemKey : keys) {
                    writeObjItem(valueMapItemKey.toString(), valueMap.get(valueMapItemKey), json);
                    json.append(STR_COMMA);
                }
                json.delete(json.length() - 1, json.length());

            }
            json.append(STR_OBJECT_RIGHT);

        } else {
            // bean

            json.append(STR_OBJECT_LEFT);
            Field[] fields = getDeclaredFields(value.getClass());
            if (fields.length > 0) {
                for (Field field : fields) {
                    Object fieldObj = getFieldObject(field, value);
                    writeObjItem(field.getName(), fieldObj, json);
                    json.append(STR_COMMA);
                }
                json.delete(json.length() - 1, json.length());
            }

            json.append(STR_OBJECT_RIGHT);
        }
    }

    public synchronized Field[] getDeclaredFields(Class<?> clazz) {
        String cacheKey = clazz.getName();
        if (cacheFields.containsKey(cacheKey)) {
            return cacheFields.get(cacheKey);
        }
        Field[] fields = getAllDeclaredFields(clazz);    //clazz.getDeclaredFields();
        cacheFields.put(cacheKey, fields);
        return fields;
    }

    private Field[] getAllDeclaredFields(Class<?> clazz) {
        List<Field> list = new ArrayList<Field>();
        Class<?> current = clazz;

        while (current != null && current != Object.class) {
            Field[] fields = current.getDeclaredFields();

            for (Field field : fields) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                list.add(field);
            }

            current = current.getSuperclass();
        }

        return list.toArray(new Field[list.size()]);
    }

    private synchronized Object getFieldObject(Field field, Object obj) {
        try {
            field.setAccessible(true);
            return field.get(obj);
        } catch (IllegalArgumentException | IllegalAccessException e) {
            logger.error(e.getMessage(), e);
            return null;
        } finally {
            field.setAccessible(false);
        }
    }


}
