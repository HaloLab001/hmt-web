# Hmt-Web

HaloMigrationTool 羲和数据库迁移工具。

从Oracle、MySQL等多种数据库迁移到羲和数据库，这包括自定义类型、表结构、约束、索引、触发器等全部数据库对象完整迁移到羲和数据库，但不同数据库的语法是有差异的，迁移的过程中可能涉及到大小写规则、数据类型和精度、分区表、触发器、序列等语法大量改造工作，还需要迁移大量的表数据，同时保证数据的完整性。

羲和数据库迁移工具致力于利用简洁直观的web操作界面来高效、稳定地完成Oracle、MySQL等多种数据库到羲和数据库之间的数据迁移功能。

# 安装要求

在开始构建和安装过程之前，请确保满足以下先决条件：

- Java8 (建议jdk1.8.0_151以上)
- Maven (建议3.6.1以上) 
- hmt 工具包
- Python2.7 (支持Python3需要修改替换hmt/bin下面的三个python文件，替换文件在doc/hmt-web/hmt-python3下)
- Halo14 或更高的版本 (适用于存储库)
- Windows、Linux

# 主要功能支持说明

- 元数据迁移: 源端Oracle、MySQL 迁移至 目标端Halo
- 全量数据同步: Oracle、MySQL、SqlServer、Db2、PostgreSql、DaMeng、Halo (都支持反向同步数据)
- 增量数据同步: Oracle(目前测试支持11g-19c)、MySQL、SqlServer、PostgreSql、Halo (目前只测试过以上数据源同步到Halo以及反向同步)
- 数据对比: 源端Oracle、MySQL 至 目标端Halo
- 一键迁移: 源端Oracle、MySQL 至 目标端Halo

# 架构说明 
 <img src="https://halo-lwl.oss-cn-hangzhou.aliyuncs.com/hmt-framework.png" style="width: 40%; height: auto;" />

- Hmt-Web: 提供简洁直观的web操作界面，通过页面完成迁移任务。
- 调度中心: 页面菜单栏提供数据源管理、任务管理、统计报表等多个功能模块。
- 执行器集群: 支持执行器多节点路由策略选择，支持超时控制、失败重试、失败告警、任务依赖，执行器CPU.内存.负载的监控等。
- Hmt: 采用Framework + plugin架构构建。将数据源读取和写入抽象成为Reader/Writer插件，纳入到整个同步框架中。
- Reader: 数据采集模块，负责采集源端数据源的数据，将数据发送至FrameWork。
- Writer: 数据写入模块，负责不断的向FrameWork取数据，并将数据写入目标端。
- FrameWork: 用于连接reader和writer，作为两者的数据传输通道，处理缓冲，流控，并发，转换等核心技术问题。

# 执行概念

将迁移工具部署后，启动hmt-web服务，通过web界面建立迁移任务并执行，通过配置过的并发度将任务批量下发到hmt数据同步模块，将不同数据源的同步抽象为从源头数据源读取数据的插件，以及向目标端写入数据的Writer插件，由FrameWork连接Reader和Writer，作为两者的数据传输通道，处理缓冲，流控，并发，转换等，Writer不断的向FrameWork取数据并写入目标端，从而完成数据迁移任务。
"# hmt-web" 
"# hmt-web" 
