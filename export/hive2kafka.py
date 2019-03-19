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

    parser.add_option("-i", "--hive", dest="hive_table", action="store", type="string", help="hive table")
    parser.add_option("-s", "--kafka_cluster", dest="kafka_cluster", action="store", help="kafka cluster")
    parser.add_option("-t", "--to", dest="kafka_topic", action="store", help="kafka topic")
    parser.add_option("-q", "--query", dest="hive_hql", action="store",help="hive query hql")
    parser.add_option("-p", "--type", dest="operation_type", action="store",
                      help="operation type:export")
    parser.add_option("-o", "--op", dest="operation", action="store",
                      help="operation")
    parser.add_option("-k", "--spark_submit_conf", dest="spark_submit_conf", action="store",
                      help="spark submit conf")
    parser.add_option("-c", "--kafka_producer_conf", dest="kafka_producer_conf", action="store",
                      help="kafka producer conf")

    return parser

'''
call bigdata-spark-project/bigdata-spark-etl-tool
'''


def run(options, args):
    try:
        kafka_cluster = options.kafka_cluster
        prefix = "kafka." + kafka_cluster
        kafka_cluster_bootstrap_servers = config_util.get(prefix + ".bootstrap.servers")

        hive2kafka_config_dict = {}
        hive2kafka_config_dict['hive_table'] = options.hive_table
        hive2kafka_config_dict['operation_type'] = options.operation_type
        hive2kafka_config_dict['operation'] = options.operation
        hive2kafka_config_dict['hive_hql'] = options.hive_hql.replace(";","")
        hive2kafka_config_dict['kafka_cluster_bootstrap_servers'] = kafka_cluster_bootstrap_servers
        hive2kafka_config_dict['kafka_topic'] = options.kafka_topic

        kafka_producer_conf = options.kafka_producer_conf
        if kafka_producer_conf is not None:
            hive2kafka_config_dict['kafka_producer_conf'] = options.kafka_producer_conf

        hive2kafka_config_json = json.dumps(hive2kafka_config_dict)

        hive2kafka_config_json_fix = hive2kafka_config_json.replace('"','\\"')

        spark_path = config_util.get("spark.path")
        spark_submit_conf = options.spark_submit_conf
        spark_submit_command = spark_path + "/bin/spark-submit " + spark_submit_conf + " " + spark_etl_class + " \"" + hive2kafka_config_json_fix + "\""

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
        print("require hive_table")
        optParser.print_help()
        sys.exit(1)
    if options.kafka_cluster is None:
        print("require kafka_cluster")
        optParser.print_help()
        sys.exit(1)
    if options.kafka_topic is None:
        print("require kafka_topic")
        optParser.print_help()
        sys.exit(1)
    code = run(options, args)
    sys.exit(code)
