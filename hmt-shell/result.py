# coding:utf-8

import yaml
import pymysql
import sys
import os


def get_result():
    sql = "select " \
          "count(1) as count_all ," \
          "count(IF(trigger_status=1 and last_handle_code=0,TRUE,NULL) ) AS count_wait," \
          "count(IF(trigger_status=1 and last_handle_code=1,TRUE,NULL) ) AS count_running," \
          "count(IF(trigger_status=1 and last_handle_code=200,TRUE,NULL) ) AS count_success," \
          "count(IF(trigger_status=1 and last_handle_code=500,TRUE,NULL) ) AS count_fail " \
          "from job_info WHERE glue_type = 'PYTHON'"
    cursor.execute(sql)
    db.commit()

    rest = cursor.fetchone()

    tplt = "{0:{3}^10}\t{1:{3}^10}\t{2:^10}"
    item = ['任务总数', '待执行', '执行中', '成功', '失败']
    print(tplt.format("条目", "总数", "百分比", chr(12288)))
    for i in range(5):
        percent = '{:.2%}'.format(rest[i] / rest[0])
        print(tplt.format(item[i], rest[i], '%s' % percent, chr(12288)))

    if rest[3] + rest[4] == rest[0]:
        print("任务完成度:100%")
        if rest[4] == 0:
            print("任务成功度:100%")
    else:
        percent = '{:.2%}'.format((rest[3] + rest[4]) / rest[0])
        print('任务进行中:%s' % percent)

    get_fail()


def get_fail():
    sql = "select job_desc , id as joblog_id, DATE_FORMAT(trigger_time,'%Y-%m-%d') from job_log where id in " \
          "(select max(id) as joblogid from job_log where job_id IN " \
          "(select id from job_info WHERE glue_type = 'PYTHON' and trigger_status=1 and last_handle_code=500)  " \
          "GROUP BY job_id)"
    cursor.execute(sql)
    db.commit()
    rest = cursor.fetchall()
    if len(rest) > 0:
        print("任务失败列表:")
        print("{:<40} {:<80}".format('表名', '日志路径'))
        for item in rest:
            logpath = os.path.join(config['job']['logpath'], item[2], str(item[1]) + ".log")
            print("{:<40} {:<80}".format(item[0], logpath))

        restart_fail()


def restart_fail():
    if len(args) == 2 and args[1] == "restart":
        print("restart")
        sql = "update job_info set last_handle_code=0 WHERE glue_type = 'PYTHON' and trigger_status=1 and last_handle_code=500 "
        cursor.execute(sql)
        db.commit()
        print("重置任务数:")
        print(cursor.rowcount)


if __name__ == '__main__':
    args = sys.argv
    with open('conf/application.yml', 'r', encoding='utf-8') as f:
        file_content = f.read()
    config = yaml.load(file_content, yaml.FullLoader)

    # 使用 cursor() 方法创建一个游标对象 cursor
    db = pymysql.connect(
        host=config['job']['mysql']['url'],
        port=3306,
        user=config['job']['mysql']['username'],
        password=config['job']['mysql']['password'],
        database=config['job']['mysql']['database']
    )
    cursor = db.cursor()

    get_result()

    # 关闭数据库连接
    db.close()
