# 安装依赖

```
pip install -r requirements.txt -i http://mirrors.aliyun.com/pypi/simple/ --trusted-host mirrors.aliyun.com 

```

# 批量创建任务

```
python job.py
```

# 查询任务执行状态

```
python result.py
```

# 重置失败任务

```
python result.py restart
```

# linux打包部署

```
# 大概要3min
pip3 install Nuitka
./build.sh
```



# DataX-Shell
安装部署顺序:
datax=>datax_web=>datax_shell




