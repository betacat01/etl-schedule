# -*- coding:utf-8 -*-
'''
用于监控业务mysql库和bi相关mysql库的连接信息是否有效，及时发现连接失败的情况，降低对正常调度的影响
'''
import sys
import os

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)

from common_util.dbutil import DBUtil
import bin.global_constant as global_constant



def get_monitor_connection_config_list():
    connection_config_list = []
    connection_config_list.append({"engine":"mysql","host":"rr-2zee29r96ig42119w.mysql.rds.aliyuncs.com","port":3306,"username":"ReadOnlyUser","password":"64fb9248a128a83fdca8ee2a3482ab00"})
    return connection_config_list

def monitor_mysql_connection(connetion_config):
    host = connection_config['host']
    port = connection_config['port']
    username = connection_config['username']
    password = connection_config['password']
    db = ''
    if connection_config.has_key('db'):
        db = connection_config['db']

    try:
        dbUtil = DBUtil(global_constant.configUtil, host, port, username, password,db)
        test_sql = 'select 1'
        rows = dbUtil.query(test_sql)
        if len(rows) == 1:
            print "ok"
            sys.exit(0)
        else:
            print "error"
            sys.exit(1)
    except Exception,e:
        print e
        print "error"
        sys.exit(1)




if __name__ ==  '__main__':
    connection_config_list = get_monitor_connection_config_list()
    for connection_config in connection_config_list:
        engine = connection_config['engine']
        if engine == 'mysql':
            monitor_mysql_connection(connection_config)

