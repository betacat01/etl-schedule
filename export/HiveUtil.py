#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import datetime
import random
import pyhs2
from QueryEngineUtil import QueryEngineUtil

class HiveUtil(QueryEngineUtil):

    def get_connection(self):
        host = self.config.get("hive.host")
        port = self.config.get("hive.port")
        username = self.config.get("hive.username")
        password = self.config.get("hive.password")
        connection = pyhs2.connect(host=host,
                                   port=int(port),
                                   authMechanism="PLAIN",
                                   user=username,
                                   password=password)
        return connection

    def run_sql_by_client(self):
        hive_home = self.config.get("hive.path")
        if hive_home is None:
            raise Exception("HIVE_HOME 环境变量没有设置")
        hive_command_bin = hive_home + "/bin/hive"
        tmpdir = self.config.get("tmp.path") + "/hqls"
        try:
            for sql in self.sqls:
                mills = datetime.datetime.now().microsecond
                rand = random.randint(1, 100)
                if not os.path.exists(tmpdir):
                    os.makedirs(tmpdir)
                tmppath = tmpdir + "/" + str(mills) + "-" + str(rand) + ".hql"
                print "tmp path " + str(tmppath)
                sys.stdout.flush()
                tmp_file = open(tmppath, "w")
                for var in self.vars:
                    tmp_file.write(var + '\n')
                    print var
                tmp_file.flush()
                tmp_file.write(sql)
                tmp_file.flush()
                tmp_file.close()

                print "start run sql: \n" + str(sql)
                sys.stdout.flush()

                hive_exec_result_code = os.system(hive_command_bin + " -f " + tmppath)
                #os.remove(tmppath)
                if hive_exec_result_code != 0:
                    print "hive run sql error exit"
                    sys.stdout.flush()
                    return -1
            return 0
        except Exception, e:
            print(e)
            sys.stdout.flush()
            return 1
