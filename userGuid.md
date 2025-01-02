
# Hmt Web用户手册

- Halo迁移工具部署手册20241230.pdf
- Halo迁移工具操作手册20241230.pdf
- 文档位置: doc/hmt-web/pdf/

# Hmt Web安装包准备

### 1. 下载官方提供的版本zip版本包

 暂时请联系迁移工具相关人员提取

### 2. 编译打包 (仍需找相关人员提取hmt工具包)

 直接从Git上面获得源代码，在项目的根目录下执行如下命令

```
  mvn clean install
```

 执行成功后将会在工程的build目录下生成安装包

```
  build/hmt-web-{VERSION}.tar.gz
```

# 部分开发环境说明

### 1. 创建Halo数据库

 执行bin/db下面的sql文件 (详细说明请查看Halo迁移工具部署手册)

### 2. 修改项目配置

### 1. 修改hmt_admin下resources/application.yml文件

```
#数据源
  datasource:
    url: jdbc:halo://127.0.0.1:1921/hmt?currentSchema=datax_web&ignore_warn=':postgresql:'
    driver-class-name: com.halo.Driver
    username: dbadmin
    password: 123456
```

 修改数据源配置，目前仅支持Halo数据库

### 2. 修改hmt_executor下resources/application.yml文件

```
#数据源
  executor:
    jsonpath: /opt/module/hmt/bin/
    #jsonpath: D:\VscodeXm\hmt\hmt\bin\

  pypath: /opt/module/hmt/bin/datax.py
  #pypath: D:\VscodeXm\hmt\hmt\bin\datax.py
```

 需要指定hmt迁移工具包的位置路径

