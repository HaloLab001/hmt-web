package com.wugui.hmt.rpc.util.json;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/*-------------------------------------------------------------------------
 *
 * BasicJson.java
 * BasicJson类
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
 *    /hmt-rpc/src/main/java/com/wugui/hmt/rpc/util/json/BasicJson.java
 *
 *-----------------------------------------------
 */
public class BasicJson {


    private static final BasicJsonReader basicJsonReader = new BasicJsonReader();
    private static final BasicJsonwriter basicJsonwriter = new BasicJsonwriter();


    /**
     * object to json
     *
     * @param object
     * @return
     */
    public static String toJson(Object object) {
        return basicJsonwriter.toJson(object);
    }

    /**
     * parse json to map
     *
     * @param json
     * @return only for filed type "null、ArrayList、LinkedHashMap、String、Long、Double、..."
     */
    public static Map<String, Object> parseMap(String json) {
        return basicJsonReader.parseMap(json);
    }

    /**
     * json to List
     *
     * @param json
     * @return
     */
    public static List<Object> parseList(String json) {
        return basicJsonReader.parseList(json);
    }


    public static void main(String[] args) {
        Map<String, Object> result = new HashMap<>();
        result.put("code", 200);
        result.put("msg", "success");
        result.put("arr", Arrays.asList("111", "222"));
        result.put("float", 1.11f);
        result.put("temp", null);

        String json = toJson(result);
        System.out.println(json);

        Map<String, Object> mapObj = parseMap(json);
        System.out.println(mapObj);

        List<Object> listInt = parseList("[111,222,33]");
        System.out.println(listInt);

    }



    /*// parse biz-object from map-object
    private static <T> T parseBizObjectFromMapObject(final Map<String, Object> mapObject, Class<T> businessClass){
        // parse class (only first level)
        try {
            Object newItem = businessClass.newInstance();
            Field[] fieldList = basicJsonwriter.getDeclaredFields(businessClass);
            for (Field field: fieldList) {

                // valid val
                Object fieldValue = mapObject.get(field.getName());
                if (fieldValue == null) {
                    continue;
                }

                // valid type
                if (field.getType() != fieldValue.getClass()) {

                    if (fieldValue instanceof LinkedHashMap) {

                        // Map-Value >> only support "class | map"
                        if (field.getType() != Map.class) {
                            fieldValue = parseBizObjectFromMapObject((LinkedHashMap)fieldValue, field.getType());
                        }
                    } else if (fieldValue instanceof ArrayList) {

                        // List-Value >> only support "List<Base> | List<Class>"
                        List<Object> fieldValueList = (ArrayList)fieldValue;
                        if (fieldValueList.size() > 0) {

                            Class list_field_RealType = (Class<?>)((ParameterizedType) field.getGenericType()).getActualTypeArguments()[0];
                            if (FieldReflectionUtil.validBaseType(list_field_RealType)) {
                                // List<Base
                                if (list_field_RealType != fieldValueList.get(0).getClass()) {

                                    List<Object> list_newItemList = new ArrayList<>();
                                    for (Object list_oldItem: fieldValueList) {

                                        Object list_newItem = FieldReflectionUtil.parseValue(list_field_RealType, String.valueOf(list_oldItem));
                                        list_newItemList.add(list_newItem);
                                    }

                                }
                            } else {
                                // List<Class>
                                fieldValue = parseBizObjectListFromMapList((ArrayList)fieldValue, list_field_RealType);
                            }
                        }

                    } else {

                        // Base-Value >> support base
                        fieldValue = FieldReflectionUtil.parseValue(field.getType(), String.valueOf(fieldValue) );
                    }
                }

                // field set
                field.setAccessible(true);
                field.set(newItem, fieldValue);
            }

            return (T) newItem;
        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse JSON", e);
        }
    }

    // parse biz-object-list from map-list
    public static <T> List<T> parseBizObjectListFromMapList(List<Object> listObject, Class<T> businessClass) {
        // valid
        if (listObject.size() == 0) {
            return new ArrayList<>();
        }
        if (listObject.get(0).getClass() != LinkedHashMap.class) {
            throw new IllegalArgumentException("Cannot parse JSON, custom class must match LinkedHashMap");
        }
        // parse business class
        try {
            List<Object> newItemList = new ArrayList<>();
            for (Object oldItem: listObject) {

                Map<String, Object> oldItemMap = (Map<String, Object>) oldItem;
                Object newItem = parseBizObjectFromMapObject(oldItemMap, businessClass);

                newItemList.add(newItem);
            }
            return (List<T>) newItemList;

        } catch (Exception e) {
            throw new IllegalArgumentException("Cannot parse JSON", e);
        }
    }


    *//**
     * parse json to <T>
     *
     * @param json
     * @param businessClass     null for base-class "Integer、Long、Map ... " , other for business-class
     * @param <T>
     * @return
     *//*
    public static <T> T parseObject(String json, Class<T> businessClass) {

        // map object
        Map<String, Object> mapObject = basicJsonReader.parseMap(json);

        if (businessClass == null || mapObject.size()==0) {
            // parse map class, default
            return (T) mapObject;
        } else {
            // parse business class
            return parseBizObjectFromMapObject(mapObject, businessClass);
        }
    }

    *//**
     * json to List<T>
     *
     * @param json
     * @param businessClass     null for base-class "Integer、Long、Map ... " , other for business-class
     * @param <T>
     * @return
     *//*
    public static <T> List<T> parseList(String json, Class<T> businessClass) {

        // list object
        List<Object> listObject = basicJsonReader.parseList(json);

        if (businessClass==null || listObject.size()==0) {
            // parse map class, default
            return (List<T>) listObject;
        } else {
            return parseBizObjectListFromMapList(listObject, businessClass);
        }

    }*/


}
