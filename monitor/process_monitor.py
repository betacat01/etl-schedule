#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import sys

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)

import socket
import commands
import urllib2
import json
import datetime
import traceback


import bin.global_constant as global_constant


class ProcessMonitor(object):

    def __init__(self):

        self.configUtil = global_constant.configUtil
        self.smsUtil = global_constant.smsUtil

        self.config = []
        # self.config.append("ip,host,namenode|datanode")

        self.config.append("172.16.0.196,hadoop-client-0-196,/data/soft/tomcat-7.0.86|CanalLauncher|CanalClientMain")
        self.config.append("172.16.0.191,hadoop-master-0-191,NameNode|ResourceManager|DFSZKFailoverController|httpfs|CanalLauncher|CanalClientMain")
        self.config.append("172.16.0.192,hadoop-master-0-192,NameNode|ResourceManager|DFSZKFailoverController|JobHistoryServer|HiveMetaStore|HiveServer2|/usr/sbin/mysqld")
        self.config.append("172.16.0.193,hadoop-worker-0-193,DataNode|NodeManager|JournalNode|QuorumPeerMain|Kafka")
        self.config.append("172.16.0.194,hadoop-worker-0-194,DataNode|NodeManager|JournalNode|QuorumPeerMain|Kafka")
        self.config.append("172.16.0.195,hadoop-worker-0-195,DataNode|NodeManager|JournalNode|QuorumPeerMain|Kafka")

    def ping(self, host):
        try:
            (status, output) = commands.getstatusoutput("ping -c 2 -w 5 " + host)
            return (status, output)
        except Exception, e:
            print(traceback.format_exc())
            return (-1, None)

    def run_command(self, user, host, process_name):
        try:
            (status, output) = commands.getstatusoutput("ssh " + user + "@" + host + " \"ps -ef | grep "
                                                        + process_name + " | grep  -v grep \"")
            return (status, output)
        except Exception, e:
            print(traceback.format_exc())
            return (-1, None)

    def get_ip(self):
        try:
            ipList = socket.gethostbyname_ex(socket.gethostname())
            (host, nothing, ip) = ipList
            return (host, str(ip[0]))
        except Exception, e:
            print(traceback.format_exc())
            return ("nothing", "nothing")

    def send_msg(self, ip, host, process):
        content = "服务器 ip:" + ip + " host:" + host + " process:" + process + " 不存在"
        response = self.smsUtil.send(",".join(["17611251600","18210775630","18612943269"]), content)
        #response = self.smsUtil.send(",".join(["15652924789"]), content)
        print("response:" + str(response))


    def monitor(self):
        print "run monitor"
        for line in self.config:
            line = line.strip()
            print line
            line_array = line.split(",")
            user = "root"
            ip = line_array[0]
            host = line_array[1]
            (status, ouput) = self.ping(ip)
            if status != 0:
                print("ip:" + ip + " host:" + host + " 服务器无法ping通")
                self.send_msg(ip, host, "ping")
            process_line = line_array[2]
            process_array = process_line.split("|")
            for process in process_array:
                (status, ouput) = self.run_command(user, host, process)
                if status != 0:
                    print("ip:" + ip + " host:" + host + " 进程:" + process + " 不存在")
                    self.send_msg(ip, host, process)
                    restart_process = RestartProcess()
                    restart_process.restart(user,host,process)


    def run(self):
            print "------------" + str(datetime.datetime.now()) + "---------------"
            self.monitor()


class RestartProcess(object):
    def __init__(self):
        self.smsUtil = global_constant.smsUtil

        self.config = {}
        self.config['HiveServer2'] = "service hive-server2 restart"
        self.config['HiveMetaStore'] = "service hive-metastore restart"
        self.config['NameNode'] = "service hadoop-hdfs-namenode restart"
        self.config['DataNode'] = "service hadoop-hdfs-datanode restart"
        self.config['JournalNode'] = "service hadoop-hdfs-journalnode restart"
        self.config['DFSZKFailoverController'] = "service hadoop-hdfs-zkfc restart"
        self.config['ResourceManager'] = "service hadoop-yarn-resourcemanager restart"
        self.config['NodeManager'] = "service hadoop-yarn-nodemanager restart"
        self.config['QuorumPeerMain'] = "service zookeeper-server restart"
        self.config['/usr/sbin/mysqld'] = "service mysqld restart"
        self.config['Kafka'] = "cd /data/soft/kafka_2.11-1.1.0;bin/kafka-server-stop.sh;bin/kafka-server-start.sh -daemon config/server.properties "
        self.config['CanalLauncher'] = "cd /data/soft/canal/canal-server;bin/stop.sh;bin/startup.sh "
        self.config['CanalClientMain'] = "cd /data/soft/canal/canal-client;bin/stop.sh;bin/startup.sh cluster wuhao1 172.16.0.193:2181,172.16.0.194:2181,172.16.0.195:2181 "


    def restart(self,user,host,process):
        if self.config.has_key(process):
            restart_cmd = "ssh " + user + "@" + host + " \""+self.config[process]+"\""
            print restart_cmd
            (status, output) = commands.getstatusoutput(restart_cmd)
            print status,output
            if status !=0 :
                receiver_array = ["17611251600","18210775630","18612943269"]
                content = "重启 " + host + " " + process + " 失败"
                self.smsUtil.send(",".join(receiver_array),content)


if __name__ == '__main__':
    monitor = ProcessMonitor()
    monitor.run()
