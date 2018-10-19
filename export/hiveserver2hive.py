#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from optparse import OptionParser
import json
import subprocess
import pyhs2
from hivetype import HiveType
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from connection import Connection
import bin.global_constant as global_constant

config_util = global_constant.configUtil


def get_option_parser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-s", "--server", dest="hiveserver2_server", action="store", help="hiveserver2 server")
    parser.add_option("-f", "--from", dest="hiveserver2_db", action="store", type="string", help="hiveserver2 database")
    parser.add_option("-t", "--to", dest="hive_db", action="store", help="hive database.table")

    parser.add_option("-c", "--columns", dest="include_columns", action="store", help="hiveserver2 table columns split by comma")
    parser.add_option("-w", "--where", dest="where", action="store", help="hiveserver2 query condition")
    parser.add_option("-q", "--query-sql", dest="query_sql", action="store", help="hiveserver2 query sql")

    parser.add_option("-p", "--partition", dest="partition", action="store", help="hive partition key=value")

    return parser


def read_base_json():
    json_str = """{
        "job": {
            "content": [
                {
                    "reader": {
                        "name": "rdbmsreader"
                    },
                    "writer": {
                        "name": "hdfswriter"
                    }
                }
            ],
            "setting": {
                "speed": {
                    "channel": "2"
                }
            }
        }
    } """.strip()
    json_data = json.loads(json_str)
    return json_data


def create_hive_table(hive_db, hive_table, partition):
    """
    创建hive表分区
    """
    connection = Connection.get_hive_connection(config_util, hive_db)
    cursor = connection.cursor()
    cursor.execute("use " + hive_db)
    cursor.execute("show tables")
    result = cursor.fetchall()
    tables = set()
    for table in result:
        tables.add(table[0])

    # if partition is None and hive_table in tables:  # 如果有partition 不能删除表,应该增加partition
    #   cursor.execute("drop table " + hive_table)
    #   tables.remove(hive_table)

    partition_key = None
    partition_value = None
    if partition is not None:
        partition_array = partition.split("=")
        partition_key = partition_array[0].strip()
        partition_value = partition_array[1].strip()

    if hive_table in tables:
        if partition_key is not None:  # 先删除再重建防止partition里面有数据
            cursor.execute(
                    "alter table " + hive_table + " drop partition(" + partition_key + "='" + partition_value + "')")
            cursor.execute(
                    "alter table " + hive_table + " add partition(" + partition_key + "='" + partition_value + "')")
    else:
        raise Exception(hive_table + " 不存在,需要先建表")

    connection.close()


def parse_hiveserver2_db(hiveserver2_db_table):
    hiveserver2_db_table_array = hiveserver2_db_table.split(".")
    hiveserver2_db = hiveserver2_db_table_array[0]
    hiveserver2_table = hiveserver2_db_table_array[1]
    return hiveserver2_db, hiveserver2_table


def parse_hive_db(hive_db_table):
    hive_db_table_array = hive_db_table.split(".")
    hive_db = hive_db_table_array[0]
    hive_table = hive_db_table_array[1]
    return hive_db, hive_table


def get_hiveserver2_results_schema(hiveserver_config_dict, db, sql):
    host = hiveserver_config_dict["host"]
    port = hiveserver_config_dict["port"]
    username = hiveserver_config_dict["username"]
    password = hiveserver_config_dict["password"]
    auth = hiveserver_config_dict["auth"]
    connection = pyhs2.connect(host=host,
                               port=int(port),
                               authMechanism=auth,
                               user=username,
                               password=password,
                               database=db)

    cursor = connection.cursor()
    cursor.execute(sql + " limit 0")
    schemas = cursor.getSchema()

    connection.close()

    column_name_type_list = []
    print "result schemas:", schemas
    for column in schemas:
        column_name_type_list.append({
            "name": column['columnName'], "type": HiveType.change_hiveserver2_type(column['type'])
        })

    return column_name_type_list


def process_hiveserver2_sql(options):
    """
    根据参数配置，获取要查询的字段
    """
    include_columns = map(lambda x: x.strip(), options.include_columns.split(","))

    where = options.where
    if where is not None:  # where 条件
        where = where.strip()

    (hiveserver2_db, hiveserver2_table) = parse_hiveserver2_db(options.hiveserver2_db)

    column_name_list = []
    for column in include_columns:
        column_name_list.append("`" + column + "`")

    # 有默认 SQL
    if options.query_sql is not None and len(options.query_sql.strip()) > 0:
        if "where" in options.query_sql:
            raise Exception("查询 SQL 的 where 条件需要添加到 where 参数上 --where")
        query_sql = options.query_sql.strip() + " where 1=1"
    else:
        query_sql = "select " + ", ".join(column_name_list) + " from `" + hiveserver2_table + "` where 1=1 "

    if where is not None and len(where) > 0:
        query_sql += " and " + where
    print "query_sql:", str(query_sql)
    return query_sql


def remove_dir(dir_name):
    print "删除 HDFS 目录:" + str(dir_name)
    os.system("hadoop fs -rmr " + dir_name)


def create_dir(dir_name):
    print "创建目录:" + str(dir_name)
    os.system("hadoop fs -mkdir -p " + dir_name)


def build_json_file(options, args):
    json_data = read_base_json()

    partition = options.partition
    if partition is not None:
        partition = partition.strip()

    (hive_db, hive_table) = parse_hive_db(options.hive_db)
    (hiveserver2_db, hiveserver2_table) = parse_hiveserver2_db(options.hiveserver2_db)
    hiveserver2_server = options.hiveserver2_server

    # 配置
    hiveserver_config_dict = Connection.get_hiverserver2_config(config_util, hiveserver2_server)

    query_sql = process_hiveserver2_sql(options)
    column_name_type_list = get_hiveserver2_results_schema(hiveserver_config_dict, hiveserver2_db, query_sql)

    jdbc_url = "jdbc:hive2://" + hiveserver_config_dict['host'] + ":" + hiveserver_config_dict['port'] \
               + hiveserver_config_dict['params'].format(database=hiveserver2_db)
    reader_params_dict = {
        "connection": [{
            "querySql": [query_sql],
            "jdbcUrl": [jdbc_url],
        }],
        "username": hiveserver_config_dict["username"],
        "password": hiveserver_config_dict["password"]
    }
    json_data["job"]["content"][0]["reader"]["parameter"] = reader_params_dict

    hive_table_path = config_util.get("hive.warehouse") + "/" + hive_db + ".db/" + hive_table
    if partition is not None:
        hive_table_path = hive_table_path + "/" + partition

    create_hive_table(hive_db, hive_table, partition)
    remove_dir(hive_table_path)
    create_dir(hive_table_path)

    writer_parameter_dict = {
        "column": column_name_type_list,
        "defaultFS": config_util.get("hdfs.uri"),
        "fieldDelimiter": '\u0001',
        "fileName": hiveserver2_table,
        "fileType": "orc",
        "path": hive_table_path,
        "writeMode": "append"
    }

    json_data["job"]["content"][0]["writer"]["parameter"] = writer_parameter_dict

    json_data["job"]["setting"]["statics"] = Connection.get_mysql_monitor_dict(config_util)

    datax_json_base_path = config_util.get("datax.json.path")
    if not os.path.exists(datax_json_base_path):
        os.makedirs(datax_json_base_path)
    datax_json_path = datax_json_base_path + "/" + hiveserver2_db + "_" + hiveserver2_table + "_" + hive_db + "_" + hive_table + ".json"
    datax_json_file_handler = open(datax_json_path, "w")
    json_str = json.dumps(json_data, indent=4, sort_keys=False, ensure_ascii=False)
    json_str = json_str.replace("\\\\", "\\")
    # print jsonStr
    datax_json_file_handler.write(json_str)
    datax_json_file_handler.close()

    print datax_json_path
    return datax_json_path


def run_datax(json_file):
    datax_command = config_util.get("datax.path") + " " + json_file
    print datax_command
    child_process = subprocess.Popen("python " + datax_command, shell=True)
    (stdout, stderr) = child_process.communicate()
    return child_process.returncode


if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')

    optParser = get_option_parser()
    options, args = optParser.parse_args(sys.argv[1:])

    print options

    if options.hiveserver2_server is None or options.hiveserver2_db is None or options.hive_db is None:
        optParser.print_help()
        sys.exit(1)
    else:
        if options.include_columns is None or len(options.include_columns) == 0:
            print("需要指定 hiveserver2 的列名")
            sys.exit(1)
        jsonFile = build_json_file(options, args)
        code = run_datax(jsonFile)
        if code != 0:
            print("datax 导入失败")
            sys.exit(1)
        else:
            complete = 0
            sys.exit(complete)
