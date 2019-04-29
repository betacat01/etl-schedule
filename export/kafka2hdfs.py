# -*- coding:utf-8 -*-
# !/usr/bin/env python

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from optparse import OptionParser
import json
import bin.global_constant as global_constant

config_util = global_constant.configUtil

def get_option_parser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-k", "--kafka_cluster", dest="kafka_cluster", action="store", type="string", help="kafka cluster")
    parser.add_option("-t", "--kafka_topic", dest="kafka_topic", action="store", help="kafka topic")
    parser.add_option("-c", "--consumer_group", dest="consumer_group", action="store", help="kafka consumer group")
    parser.add_option("-s", "--hdfs_path", dest="hdfs_path", action="store",help="hdfs path")
    parser.add_option("-p", "--type", dest="operation_type", action="store",
                      help="operation type:export")
    parser.add_option("-o", "--op", dest="operation", action="store",
                      help="operation")
    parser.add_option("-f", "--kafka_consumer_conf", dest="kafka_consumer_conf", action="store",
                      help="kafka consumer conf")

    return parser

'''
call hadoop jar kafka-hadoop-loader.jar --zk-connect [kafka_zk_cluster] --topics [kafka_topic] --consumer-group [consumer_group] [hdfs_path]
'''


def run(options, args):
    try:
        kafka_cluster = options.kafka_cluster
        prefix = "kafka." + kafka_cluster
        kafka_cluster_zookeeper_servers = config_util.get(prefix + ".zookeeper.servers")
        kafka_cluster_bootstrap_servers = config_util.get(prefix + ".bootstrap.servers")

        kafka2hdfs_config_dict = {}
        kafka2hdfs_config_dict['operation_type'] = options.operation_type
        kafka2hdfs_config_dict['operation'] = options.operation
        kafka2hdfs_config_dict['kafka_topic'] = options.kafka_topic
        kafka2hdfs_config_dict['consumer_group'] = options.consumer_group
        kafka2hdfs_config_dict['hdfs_path'] = options.hdfs_path
        kafka2hdfs_config_dict['kafka_cluster_zookeeper_servers'] = kafka_cluster_zookeeper_servers
        kafka2hdfs_config_dict['kafka_cluster_bootstrap_servers'] = kafka_cluster_bootstrap_servers

        kafka_consumer_conf = options.kafka_consumer_conf
        if kafka_consumer_conf is not None:
            kafka2hdfs_config_dict['kafka_consumer_conf'] = options.kafka_consumer_conf

        kafka2hdfs_config_json = json.dumps(kafka2hdfs_config_dict)

        kafka2hdfs_config_json_fix = kafka2hdfs_config_json.replace('"','\\"')

        print(kafka2hdfs_config_json_fix)

        hadoop_bin_path = config_util.get("hadoop.bin.path")

        kafka_hadoop_loader_jar_path = config_util.get("kafka_hadoop_loader_jar_path")

        kafka_hadoop_loader_submit_command = hadoop_bin_path + " jar " + kafka_hadoop_loader_jar_path + " --zk-connect " + kafka_cluster_zookeeper_servers + " --topics " + options.kafka_topic + " --consumer-group " + options.consumer_group + " " + options.hdfs_path

        print("kafka_hadoop_loader_submit_command:" + str(kafka_hadoop_loader_submit_command))

        code = os.system(kafka_hadoop_loader_submit_command)
        return code
    except Exception, e:
        print(e)
        return -1

if __name__ == "__main__":
    reload(sys)
    sys.setdefaultencoding('utf-8')

    optParser = get_option_parser()

    options, args = optParser.parse_args(sys.argv[1:])

    if options.kafka_cluster is None:
        print("require kafka_cluster")
        optParser.print_help()
        sys.exit(1)
    if options.kafka_topic is None:
        print("require kafka_topic")
        optParser.print_help()
        sys.exit(1)
    if options.consumer_group is None:
        print("require consumer_group")
        optParser.print_help()
        sys.exit(1)
    if options.hdfs_path is None:
        print("require hdfs_path")
        optParser.print_help()
        sys.exit(1)
    code = run(options, args)
    sys.exit(code)
