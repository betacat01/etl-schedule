# -*- coding:utf-8 -*-
# !/usr/bin/python


import os
import sys

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)

import MySQLdb
import bin.global_constant as global_constant
from common_util.dbutil import DBUtil



'''
获取连接
'''


def get_zeus_connection():
    host = global_constant.configUtil.get("zeus.mysql.host")
    username = global_constant.configUtil.get("zeus.mysql.username")
    password = global_constant.configUtil.get("zeus.mysql.password")
    port = global_constant.configUtil.get("zeus.mysql.port")
    db = global_constant.configUtil.get("zeus.mysql.db")
    dbUtil = DBUtil(global_constant.configUtil, host, port, username, password, db)
    connection = dbUtil.get_connection()
    return connection


'''
获取admin 用户
'''


def get_admin_user():
    zeus_connection = get_zeus_connection()
    sql = """select phone from zeus_user where is_effective = 1 and uid !='admin' """
    cursor = zeus_connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql, ())
    rows = cursor.fetchall()
    phones = set()
    for row in rows:
        phones.add(row["phone"])
    zeus_connection.close()
    return phones


'''
检查今天是否有running任务
'''


def check_running_job(action_id, start_hour=20, end_hour=23):

    print "检查运行中任务"

    current_hour = global_constant.dateUtil.get_time_hour(global_constant.dateUtil.get_now())
    if current_hour >= start_hour and current_hour < end_hour:
        zeus_connection = get_zeus_connection()
        cursor = zeus_connection.cursor(MySQLdb.cursors.DictCursor)
        sql = """select t1.name,t1.job_id,t1.status,t2.owner from zeus_action t1
                inner join zeus_job t2 on t1.job_id = t2.id where t1.id >= %s and t1.status = 'running'"""
        cursor.execute(sql, (action_id,))
        rows = cursor.fetchall()
        msg = list()
        for row in rows:
            msg.append("id:" + str(row["job_id"]) + "名称:" + str(row["name"]) +
                       "状态为running 当前时间:" + str(global_constant.dateUtil.get_now()) +
                       "所有人:" + str(row["owner"]) + " 需要置为success/failed")

        zeus_connection.close()

        if len(msg) > 0:
            content = ",\n".join(msg)
            print("msg:%s" % content)
            admins = get_admin_user()
            send_msg(admins, content)
    else:
        print("当前时间 %s 不在[%s,%s) 范围内,无需监控" % (current_hour, start_hour, end_hour))


'''
检查任务运行时间 默认 90min
'''


def check_time_job(threshold=120):

    print "检查任务运行时长"

    date_str = global_constant.dateUtil.get_now_fmt()
    zeus_connection = get_zeus_connection()
    cursor = zeus_connection.cursor(MySQLdb.cursors.DictCursor)
    sql = """select t1.job_id,t1.start_time,t2.name,t2.owner from zeus_action_history t1 inner join zeus_job t2
             on t1.job_id = t2.id where t1.start_time > %s and t1.status = 'running'"""
    cursor.execute(sql, (date_str,))
    rows = cursor.fetchall()
    msg = list()
    for row in rows:
        start_time = row["start_time"]
        duration = global_constant.dateUtil.get_now() - start_time
        if duration.seconds > threshold * 60:
            msg.append("id:" + str(row["job_id"]) + " 名称:" + str(row["name"]) +
                       " 启动时间:" + str(start_time) + " 运行时间超过" +
                       str(threshold) + "min 所有人:" + str(row["owner"]))
    zeus_connection.close()
    if len(msg) > 0:
        content = (",\n".join(msg))
        print("msg:%s" % content)
        admins = get_admin_user()
        send_msg(admins, content)
    else:
        #print("任务运行正常")
        pass


def get_zeus_job(connection, job_name):
    sql = "select id,name,owner from zeus_job where name = %s"
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql, (job_name,))
    row = cursor.fetchone()
    return row


def get_zeus_action_history(connection, action_id, job_id):
    sql = "select status,start_time,end_time from zeus_action_history " \
          "where action_id > %s and job_id = %s and trigger_type = 1 order by id desc limit 1"
    cursor = connection.cursor(MySQLdb.cursors.DictCursor)
    cursor.execute(sql, (action_id, job_id))
    row = cursor.fetchone()
    return row


def check_exception_runtime_job(file_path, action_id):

    print "检查job 最早最晚运行时间"

    current_time = global_constant.dateUtil.get_now()
    msg = list()

    file_handler = open(file_path, 'r')
    if not os.path.exists(file_path):
        print("文件" + file_path + " 不存在")
        return
    line_list = list()
    for line in file_handler.readlines():
        if line is not None and len(line) > 0:
            line_list.append(line)
    if len(line_list) == 0:
        print("配置内容为空")
        return

    zeus_connection = get_zeus_connection()
    for line in line_list:
        line_array = line.split(",")
        if len(line_array) != 4:
            print(line + " 配置格式不正确")
            continue

        config_job = line_array[0].strip()
        config_job_last_start_time = line_array[2].strip()
        config_job_last_end_time = line_array[3].strip()

        config_job_last_start_hour = int(config_job_last_start_time.split(":")[0])
        config_job_last_start_minute = int(config_job_last_start_time.split(":")[1])

        config_job_last_end_hour = int(config_job_last_end_time.split(":")[0])
        config_job_last_end_minute = int(config_job_last_end_time.split(":")[1])

        zeus_job = get_zeus_job(zeus_connection, config_job)
        if zeus_job is None:
            print("zeus job 不存在:" + zeus_job)
            continue
        else:
            zeus_job_id = zeus_job["id"]
            zeus_job_owner = zeus_job["owner"]

            start_replace_time = current_time.replace(hour=config_job_last_start_hour,
                                                      minute=config_job_last_start_minute)

            end_replace_time = current_time.replace(hour=config_job_last_end_hour,
                                                    minute=config_job_last_end_minute)
            #print("检查job: %s 最晚开始运行时间:%s  最晚运行结束时间: %s" % (zeus_job, start_replace_time, end_replace_time))

            zeus_action_history = get_zeus_action_history(zeus_connection, action_id, zeus_job_id)
            if zeus_action_history is None:
                print("job id: %s 对应action_history不存在" % (zeus_job_id,))
                if current_time > start_replace_time:
                    msg.append("job: " + str(zeus_job["name"]) + " 在 " + str(start_replace_time) + " 未开始运行 所有人:"
                               + str(zeus_job_owner))
            else:
                action_history_status = zeus_action_history["status"]
                end_time = zeus_action_history["end_time"]
                if current_time > end_replace_time and action_history_status == "running":
                    msg.append("job: " + str(zeus_job["name"]) + " 在 " + str(end_replace_time) + " 未结束运行 所有人:"
                               + str(zeus_job_owner))

    if len(msg) > 0:
        content = (",\n".join(msg))
        print("msg:%s" % content)
        admins = get_admin_user()
        send_msg(admins, content)


def check_complete_job_cnt(file_path, action_id):

    print "检查某时刻任务数"

    current_time = global_constant.dateUtil.get_now()

    current_hour = current_time.hour
    current_minute = current_time.minute

    msg = list()

    file_handler = open(file_path, 'r')
    if not os.path.exists(file_path):
        print("文件" + file_path + " 不存在")
        return
    line_list = list()
    for line in file_handler.readlines():
        if line is not None and len(line) > 0:
            line_list.append(line)
    if len(line_list) == 0:
        print("配置内容为空")
        return

    for line in file_handler.readlines():
        if line is not None and len(line) > 0:
            line_list.append(line)
    if len(line_list) == 0:
        print("配置内容为空")
        return

    for line in line_list:
        line_array = line.split(",")
        if len(line_array) != 2:
            print(line + " 配置格式不正确")
            continue

        should_line = line_array[0]
        should_count = int(line_array[1])

        should_time_hour = int(should_line.split(":")[0])
        should_time_minute = int(should_line.split(":")[1])

        if should_time_hour == current_hour and should_time_minute == current_minute :
            zeus_connection = get_zeus_connection()
            cursor = zeus_connection.cursor(MySQLdb.cursors.DictCursor)
            sql = "select status,count(distinct(job_id)) as job_count from zeus_action where id > %s  group by status "
            cursor.execute(sql, (action_id,))
            rows = cursor.fetchall()

            running_cnt = 0
            success_cnt = 0
            for row in rows:
                status = row["status"]
                if status == "success":
                    success_cnt = row["job_count"]
                if status == "running":
                    running_cnt = row["job_count"]

            if success_cnt < should_count:
                msg.append("当前时间:" + str(current_time) + " 运行完成job的数量:" + str(success_cnt) + " 小于" + str(should_count))

            if should_count == -1 and running_cnt > 0:
                msg.append("当前时间:" + str(current_time) + " 正在运行job的数量:" + str(running_cnt) + " 未全部运行完成")

    if len(msg) > 0:
        content = (",\n".join(msg))
        print("msg:%s" % content)
        admins = get_admin_user()
        send_msg(admins, content)



'''
发送短信
'''


def send_msg(admins, content):
    response = global_constant.smsUtil.send(",".join(admins), content)
    print(response)
    return response
    #return 0

def run():

    base = global_constant.project_path + os.sep + "monitor"
    date_str = global_constant.dateUtil.get_now_fmt("%Y%m%d")
    action_id = date_str + "0" * 10

    check_running_job(action_id)

    check_time_job()

    exception_runtime_job_path = base + "/zeus_job_exception_runtime.txt"
    check_exception_runtime_job(file_path=exception_runtime_job_path, action_id=action_id)

    complete_job_cnt_job_path = base + "/zeus_job_complete_cnt.txt"
    check_complete_job_cnt(file_path=complete_job_cnt_job_path, action_id=action_id)

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    run()

