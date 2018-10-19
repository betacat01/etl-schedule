# -*- coding:utf-8 -*-
'''
测试 hive-server2 客户端
'''
import pyhs2
import global_constant as global_constant

configUtil = global_constant.configUtil

def hive_connection(db):
    host = configUtil.get("hive.host")
    port = configUtil.get("hive.port")
    username = configUtil.get("hive.username")
    password = configUtil.get("hive.password")
    connection = pyhs2.connect(host=host,
                               port=int(port),
                               authMechanism="PLAIN",
                               user=username,
                               password=password,
                               database=db)
    return connection

if __name__ =='__main__':
    sql = "select * from dim.dim_date"
    connection = hive_connection(None)
    cursor = connection.cursor()
    cursor.execute(sql)
    cols = cursor.getSchema()
    for col in cols:
        print col
    rows = cursor.fetch()
    for row in rows:
        print row
