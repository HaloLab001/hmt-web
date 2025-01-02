# coding:utf-8

import yaml
import requests
import json
import pymysql


def test_jdbc():
    url_jdbc = webapi + '/api/jobJdbcDatasource/test'
    response = requests.post(url_jdbc, headers=headers, data=json.dumps(from_jdbc))
    res = response.json()

    if (res['code'] != 0):
        print("from数据源连接异常")
        print(res['msg'])
        exit(0)

    response = requests.post(url_jdbc, headers=headers, data=json.dumps(to_jdbc))
    res = response.json()

    if (res['code'] != 0):
        print("to数据源连接异常")
        print(res['msg'])
        exit(0)

    print("test数据源连接正常")


def test_schema():
    print("test数据源schema正常")
    from_table = conf_from['table']
    if type(from_table) is list:
        print('table list')
        for table in from_table:
            print(table)
            build_json(table.get("name"), table.get("splitPk"))
    elif type(from_table) is str and from_table == "all":
        print('table all')
        from_jdbc['databaseName'] = conf_from['schema']

        url_schema = webapi + '/api/metadata/getTablesPy'
        response = requests.post(url_schema, headers=headers, data=json.dumps(from_jdbc))
        res = response.json()

        if (res['code'] != 0):
            print("from数据源获取schema下的系统表异常")
            print(res['msg'])
            exit(0)
        table_list = res['data']
        print(table_list)
        for table in table_list:
            print(table)
            build_json(table)
    else:
        print('from_table配置异常')


def build_json(table, pk=''):
    if conf_from['type'] not in reader.keys():
        print("from 数据源type不支持")
        exit(0)

    if table.find(".") == -1:
        table_from = conf_from['schema'] + "." + table
        table_to = conf_to['schema'] + "." + table
    else:
        table_from, table_to = table, table

    job['job']['content'][0]['reader']['parameter']['connection'][0]['table'] = [table_from]
    if pk != '':
        job['job']['content'][0]['reader']['parameter']['splitPk'] = pk

    job['job']['content'][0]['writer']['parameter']['connection'][0]['table'] = [table_to]

    print(job)

    sql = "insert into job_info (job_group,job_cron,job_desc,project_id,user_id,executor_route_strategy,executor_handler,executor_block_strategy,glue_type,job_json,trigger_status) values (1,'',%s,0,1,'RANDOM','executorJobHandler','DISCARD_LATER','PYTHON',%s,1)"

    cursor.execute(sql, (table, json.dumps(job, ensure_ascii=False, indent=2)))

    db.commit()


if __name__ == '__main__':
    with open('conf/application.yml', 'r', encoding='utf-8') as f:
        file_content = f.read()
    config = yaml.load(file_content, yaml.FullLoader)
    with open("template.json", 'r') as f:
        job = json.load(f)

    conf_from = config['datasource']['from']
    conf_to = config['datasource']['to']
    webapi = config['job']['webapi']
    headers = {'Content-Type': 'application/json'}

    from_jdbc = {
        'datasource': conf_from['type'],
        'jdbcUsername': conf_from['jdbc_username'],
        'jdbcPassword': conf_from['jdbc_password'],
        'jdbcUrl': conf_from['jdbc_url'],
        'jdbcDriverClass': conf_from['jdbc_driver_class']
    }

    to_jdbc = {
        'datasource': conf_to['type'],
        'jdbcUsername': conf_to['jdbc_username'],
        'jdbcPassword': conf_to['jdbc_password'],
        'jdbcUrl': conf_to['jdbc_url'],
        'jdbcDriverClass': conf_to['jdbc_driver_class']
    }

    reader = {
        "oracle": "oraclereader",
        "mysql": "mysqlreader",
        "halo": "haloreader",
        "sqlserver": "sqlserverreader",
        "postgresql": "postgresqlreader",
        "db2": "rdbmsreader"
    }

    job['job']['content'][0]['reader']['name'] = reader[conf_from['type']]
    job['job']['content'][0]['reader']['parameter']['username'] = conf_from['jdbc_username']
    job['job']['content'][0]['reader']['parameter']['password'] = conf_from['jdbc_password']
    job['job']['content'][0]['reader']['parameter']['connection'][0]['jdbcUrl'] = [conf_from['jdbc_url']]

    job['job']['content'][0]['writer']['name'] = 'halobatchwriter'
    job['job']['content'][0]['writer']['parameter']['username'] = conf_to['jdbc_username']
    job['job']['content'][0]['writer']['parameter']['password'] = conf_to['jdbc_password']
    job['job']['content'][0]['writer']['parameter']['connection'][0]['jdbcUrl'] = conf_to['jdbc_url']

    print(config['job']['mysql'])
    # exit(0)

    # 使用 cursor() 方法创建一个游标对象 cursor
    db = pymysql.connect(
        host=config['job']['mysql']['url'],
        port=3306,
        user=config['job']['mysql']['username'],
        password=config['job']['mysql']['password'],
        database=config['job']['mysql']['database']
    )
    cursor = db.cursor()

    print(reader)
    print(webapi)
    test_jdbc()
    test_schema()

    # 关闭数据库连接
    db.close()

    print("end")
