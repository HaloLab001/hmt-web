package com.wugui.hmt.admin.config;

import com.google.common.collect.Multimap;
import io.swagger.models.*;
import io.swagger.models.parameters.Parameter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.context.MessageSource;
import org.springframework.context.annotation.Primary;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.stereotype.Component;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.service.ApiListing;
import springfox.documentation.service.Documentation;
import springfox.documentation.service.ResourceListing;
import springfox.documentation.swagger2.mappers.*;

import java.util.*;

import static com.google.common.collect.Maps.newTreeMap;


/*-------------------------------------------------------------------------
 *
 * ServiceModelToSwagger2MapperImpl.java
 *   application configuration
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
 *	  hmt-admin/src/main/java/com/wugui/hmt/admin/config/ServiceModelToSwagger2MapperImpl.java
 *
 *-------------------------------------------------------------------------
 */
@Component(value = "ServiceModelToSwagger2Mapper")
@Primary
@ConditionalOnWebApplication
public class ServiceModelToSwagger2MapperImpl extends ServiceModelToSwagger2Mapper {


    @Autowired
    private ModelMapper modelMapper;
    @Autowired
    private ParameterMapper parameterMapper;
    @Autowired
    private SecurityMapper securityMapper;
    @Autowired
    private LicenseMapper licenseMapper;
    @Autowired
    private VendorExtensionsMapper vendorExtensionsMapper;

    @Autowired
    private MessageSource messageSource;

    @Override
    public Swagger mapDocumentation(Documentation from) {

        if (from == null) {
            return null;
        }

        Swagger swagger = new Swagger();

        swagger.setVendorExtensions(vendorExtensionsMapper.mapExtensions(from.getVendorExtensions()));
        swagger.setSchemes(mapSchemes(from.getSchemes()));
        swagger.setPaths(mapApiListings(from.getApiListings()));
        swagger.setHost(from.getHost());
        swagger.setDefinitions(modelsFromApiListings( from.getApiListings() ) );
        swagger.setSecurityDefinitions(securityMapper.toSecuritySchemeDefinitions(from.getResourceListing()));
        ApiInfo info = fromResourceListingInfo(from);
        if (info != null) {
            swagger.setInfo(mapApiInfo(info));
        }
        swagger.setBasePath(from.getBasePath());
        swagger.setTags(tagSetToTagList(from.getTags()));
        List<String> list2 = from.getConsumes();
        if (list2 != null) {
            swagger.setConsumes(new ArrayList<String>(list2));
        } else {
            swagger.setConsumes(null);
        }
        List<String> list3 = from.getProduces();
        if (list3 != null) {
            swagger.setProduces(new ArrayList<String>(list3));
        } else {
            swagger.setProduces(null);
        }

        return swagger;
    }


    @Override
    protected Info mapApiInfo(ApiInfo from) {

        if (from == null) {
            return null;
        }

        Info info = new Info();

        info.setLicense(licenseMapper.apiInfoToLicense(from));
        info.setVendorExtensions(vendorExtensionsMapper.mapExtensions(from.getVendorExtensions()));
        info.setTermsOfService(from.getTermsOfServiceUrl());
        info.setContact(map(from.getContact()));
        info.setDescription(from.getDescription());
        info.setVersion(from.getVersion());
        info.setTitle(from.getTitle());

        return info;
    }

    @Override
    protected Contact map(springfox.documentation.service.Contact from) {

        if (from == null) {
            return null;
        }

        Contact contact = new Contact();

        contact.setName(from.getName());
        contact.setUrl(from.getUrl());
        contact.setEmail(from.getEmail());

        return contact;
    }

    @Override
    protected Operation mapOperation(springfox.documentation.service.Operation from) {

        if (from == null) {
            return null;
        }

        Locale locale = LocaleContextHolder.getLocale();

        Operation operation = new Operation();

        operation.setSecurity(mapAuthorizations(from.getSecurityReferences()));
        operation.setVendorExtensions(vendorExtensionsMapper.mapExtensions(from.getVendorExtensions()));
        operation.setDescription(messageSource.getMessage(from.getNotes(), null, from.getNotes(), locale));
        operation.setOperationId(from.getUniqueId());
        operation.setResponses(mapResponseMessages(from.getResponseMessages()));
        operation.setSchemes(stringSetToSchemeList(from.getProtocol()));
        Set<String> tagsSet = new HashSet<>(1);

        if(from.getTags() != null && from.getTags().size() > 0){

            List<String> list = new ArrayList<String>(tagsSet.size());

            Iterator<String> it = from.getTags().iterator();
            while(it.hasNext()){
               String tag = it.next();
               list.add(
                   StringUtils.isNotBlank(tag) ? messageSource.getMessage(tag, null, tag, locale) : " ");
            }

            operation.setTags(list);
        }else {
            operation.setTags(null);
        }

        operation.setSummary(from.getSummary());
        Set<String> set1 = from.getConsumes();
        if (set1 != null) {
            operation.setConsumes(new ArrayList<String>(set1));
        } else {
            operation.setConsumes(null);
        }

        Set<String> set2 = from.getProduces();
        if (set2 != null) {
            operation.setProduces(new ArrayList<String>(set2));
        } else {
            operation.setProduces(null);
        }


        operation.setParameters(parameterListToParameterList(from.getParameters()));
        if (from.getDeprecated() != null) {
            operation.setDeprecated(Boolean.parseBoolean(from.getDeprecated()));
        }

        return operation;
    }

    @Override
    protected Tag mapTag(springfox.documentation.service.Tag from) {

        if (from == null) {
            return null;
        }

        Locale locale = LocaleContextHolder.getLocale();

        Tag tag = new Tag();

        tag.setVendorExtensions(vendorExtensionsMapper.mapExtensions(from.getVendorExtensions()));
        tag.setName(messageSource.getMessage(from.getName(), null, from.getName(), locale));
        tag.setDescription(from.getDescription());

        return tag;
    }


    private ApiInfo fromResourceListingInfo(Documentation documentation) {

        if (documentation == null) {
            return null;
        }
        ResourceListing resourceListing = documentation.getResourceListing();
        if (resourceListing == null) {
            return null;
        }
        ApiInfo info = resourceListing.getInfo();
        if (info == null) {
            return null;
        }
        return info;
    }

    protected List<Tag> tagSetToTagList(Set<springfox.documentation.service.Tag> set) {

        if (set == null) {
            return null;
        }

        List<Tag> list = new ArrayList<Tag>(set.size());
        for (springfox.documentation.service.Tag tag : set) {
            list.add(mapTag(tag));
        }

        return list;
    }

    protected List<Scheme> stringSetToSchemeList(Set<String> set) {
        if (set == null) {
            return null;
        }

        List<Scheme> list = new ArrayList<Scheme>(set.size());
        for (String string : set) {
            list.add(Enum.valueOf(Scheme.class, string));
        }

        return list;
    }

    protected List<Parameter> parameterListToParameterList(List<springfox.documentation.service.Parameter> list) {
        if (list == null) {
            return null;
        }

        List<Parameter> list1 = new ArrayList<Parameter>(list.size());

        Locale locale = LocaleContextHolder.getLocale();

        for (springfox.documentation.service.Parameter param : list) {
            String description = messageSource.getMessage(param.getDescription(), null, param.getDescription(), locale);

            springfox.documentation.service.Parameter parameter = new springfox.documentation.service.Parameter(param.getName(),description,param.getDefaultValue(),param.isRequired(),param.isAllowMultiple(),param.isAllowEmptyValue(),param.getModelRef(),param.getType(),param.getAllowableValues(),param.getParamType(),param.getParamAccess(),param.isHidden(),param.getPattern(),param.getCollectionFormat(),param.getOrder(),param.getScalarExample(),param.getExamples() ,param.getVendorExtentions());
            list1.add(parameterMapper.mapParameter(parameter));
        }

        return list1;
    }


    Map<String, Model> modelsFromApiListings(Multimap<String, ApiListing> apiListings) {
        Map<String, springfox.documentation.schema.Model> definitions = newTreeMap();
        for (ApiListing each : apiListings.values()) {
            definitions.putAll(each.getModels());
        }
        return modelMapper.mapModels(definitions);
    }

}
