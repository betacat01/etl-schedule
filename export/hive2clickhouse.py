# -*- coding:utf-8 -*-
# !/usr/bin/env python

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from optparse import OptionParser
import json
import bin.global_constant as global_constant

config_util = global_constant.configUtil

spark_etl_class = config_util.get("spark_etl_class")

def get_option_parser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-d", "--delete_sql", dest="delete_sql", action="store", type="string", help="clickhouse delete sql")
    parser.add_option("-i", "--hive", dest="hive_table", action="store", type="string", help="hive table")
    parser.add_option("-s", "--clickhouse_cluster", dest="clickhouse_cluster", action="store", help="clickhouse cluster")
    parser.add_option("-t", "--to", dest="clickhouse_db_table", action="store", help="clickhouse db.table")
    parser.add_option("-q", "--query", dest="hive_hql", action="store",help="hive query hql")
    parser.add_option("-c", "--columns", dest="clickhouse_columns", action="store",
                      help="clickhouse table columns split by comma")
    parser.add_option("-p", "--type", dest="operation_type", action="store",
                      help="operation type:export")
    parser.add_option("-o", "--op", dest="operation", action="store",
                      help="operation")
    parser.add_option("-k", "--spark_submit_conf", dest="spark_submit_conf", action="store",
                      help="spark submit conf")

    return parser

'''
call bigdata-spark-project/bigdata-spark-etl-tool
'''


def run(options, args):
    try:
        clickhouse_cluster = options.clickhouse_cluster
        prefix = "clickhouse." + clickhouse_cluster
        clickhouse_cluster_url = config_util.get(prefix + ".url")
        clickhouse_username = config_util.getOrElse(prefix + ".username",None)
        clickhouse_password = config_util.getOrElse(prefix + ".password",None)

        hive2clickhouse_config_dict = {}
        hive2clickhouse_config_dict['delete_sql'] = options.delete_sql
        hive2clickhouse_config_dict['hive_table'] = options.hive_table
        hive2clickhouse_config_dict['clickhouse_db_table'] = options.clickhouse_db_table
        hive2clickhouse_config_dict['clickhouse_database'] = options.clickhouse_db_table.split(".")[0]
        hive2clickhouse_config_dict['clickhouse_db_table'] = options.clickhouse_db_table
        hive2clickhouse_config_dict['clickhouse_columns'] = options.clickhouse_columns
        hive2clickhouse_config_dict['operation_type'] = options.operation_type
        hive2clickhouse_config_dict['operation'] = options.operation
        hive2clickhouse_config_dict['hive_hql'] = options.hive_hql.replace(";","")
        hive2clickhouse_config_dict['clickhouse_cluster_url'] = clickhouse_cluster_url
        if clickhouse_username is not None:
            hive2clickhouse_config_dict['clickhouse_username'] = clickhouse_username
            hive2clickhouse_config_dict['clickhouse_password'] = clickhouse_password

        hive2clickhouse_config_json = json.dumps(hive2clickhouse_config_dict)

        hive2clickhouse_config_json_fix = hive2clickhouse_config_json.replace('"','\\"')

        spark_path = config_util.get("spark.path")
        spark_submit_conf = options.spark_submit_conf
        spark_submit_command = spark_path + "/bin/spark-submit " + spark_submit_conf + " " + spark_etl_class + " \"" + hive2clickhouse_config_json_fix + "\""

        print("spark_submit_command:" + str(spark_submit_command))

        code = os.system(spark_submit_command)
        return code
    except Exception, e:
        print(e)
        return -1

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')

    optParser = get_option_parser()

    options, args = optParser.parse_args(sys.argv[1:])

    if options.hive_table is None:
        print("require hive table")
        optParser.print_help()
        sys.exit(1)
    if options.clickhouse_cluster is None:
        print("require clickhouse cluster")
        optParser.print_help()
        sys.exit(1)
    if options.clickhouse_db_table is None:
        print("require clickhouse_db_table")
        optParser.print_help()
        sys.exit(1)
    if options.clickhouse_columns is None:
        print("require clickhouse_columns")
        optParser.print_help()
        sys.exit(1)
    code = run(options, args)
    sys.exit(code)
