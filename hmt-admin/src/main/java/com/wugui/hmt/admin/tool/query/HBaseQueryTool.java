package com.wugui.hmt.admin.tool.query;


import com.wugui.hmt.core.util.Constants;
import com.wugui.hmt.admin.core.util.LocalCacheUtil;
import com.wugui.hmt.admin.entity.JobDatasource;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*-------------------------------------------------------------------------
 *
 * HBaseQueryTool.java
 *  HBase数据库使用的查询工具
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
 *    hmt-admin/src/main/java/com/wugui/hmt/admin/tool/query/HBaseQueryTool.java
 *
 *-----------------------------------------------
 */
public class HBaseQueryTool {

    private Configuration conf = HBaseConfiguration.create();
    private ExecutorService pool = Executors.newScheduledThreadPool(2);
    private Connection connection = null;
    private Admin admin;
    private Table table;

    public HBaseQueryTool(JobDatasource jobDatasource) throws IOException {
        if (LocalCacheUtil.get(jobDatasource.getDatasourceName()) == null) {
            getDataSource(jobDatasource);
        } else {
            connection = (Connection) LocalCacheUtil.get(jobDatasource.getDatasourceName());
            if (connection == null || connection.isClosed()) {
                LocalCacheUtil.remove(jobDatasource.getDatasourceName());
                getDataSource(jobDatasource);
            }
        }
        LocalCacheUtil.set(jobDatasource.getDatasourceName(), connection, 4 * 60 * 60 * 1000);
    }

    private void getDataSource(JobDatasource jobDatasource) throws IOException {
        String[] zkAdress = jobDatasource.getZkAdress().split(Constants.SPLIT_SCOLON);
        conf.set("hbase.zookeeper.quorum", zkAdress[0]);
        conf.set("hbase.zookeeper.property.clientPort", zkAdress[1]);
        connection = ConnectionFactory.createConnection(conf, pool);
        admin = connection.getAdmin();
    }

    // 关闭连接
    public void sourceClose() {
        try {
            if (admin != null) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
            if (table != null) {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试是否连接成功
     *
     * @return
     * @throws IOException
     */
    public boolean dataSourceTest() throws IOException {
        Admin admin = connection.getAdmin();
        HTableDescriptor[] tableDescriptor = admin.listTables();
        return tableDescriptor.length > 0;
    }

    /**
     * 获取HBase表名称
     *
     * @return
     * @throws IOException
     */
    public List<String> getTableNames() throws IOException {
        List<String> list = new ArrayList<>();
        Admin admin = connection.getAdmin();
        TableName[] names = admin.listTableNames();
        for (int i = 0; i < names.length; i++) {
            list.add(names[i].getNameAsString());
        }
        return list;
    }

    /**
     * 通过表名查询所有l列祖和列
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public List<String> getColumns(String tableName) throws IOException {
        List<String> list = new ArrayList<>();
        table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan();
        //Filter filter = new PageFilter(1);
        //scan.setFilter(filter);
        scan.getStartRow();
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> it = scanner.iterator();
        if (it.hasNext()) {
            Result re = it.next();
            List<Cell> listCells = re.listCells();
            for (Cell cell : listCells) {
                list.add(new String(CellUtil.cloneFamily(cell)) + ":" + new String(CellUtil.cloneQualifier(cell)));
            }
        }
        return list;
    }
}
