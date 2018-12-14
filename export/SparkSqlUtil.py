#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import datetime
import random
import pyhs2
from QueryEngineUtil import QueryEngineUtil

class SparkSqlUtil(QueryEngineUtil):
    def get_connection(self):
        host = self.config.get("spark.thrift.host")
        port = self.config.get("spark.thrift.port")
        username = self.config.get("spark.thrift.username")
        password = self.config.get("spark.thrift.password")
        connection = pyhs2.connect(host=host,
                                   port=int(port),
                                   authMechanism="PLAIN",
                                   user=username,
                                   password=password)
        return connection

    def run_sql_by_client(self):
        spark_home = self.config.get("spark.path")
        if spark_home is None:
            raise Exception("spark.path 没有设置")
        spark_sql_opt = self.config.get("spark.sql.opt")
        spark_sql_command_bin = spark_home + "/bin/spark-sql "
        tmpdir = self.config.get("tmp.path") + "/hqls"

        hive_home = self.config.get("hive.path")
        hive_command_bin = hive_home + "/bin/hive"

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
                spark_exe_cmd = spark_sql_command_bin + spark_sql_opt + " -f " + tmppath
                print spark_exe_cmd
                spark_sql_exec_result_code = os.system(spark_exe_cmd)
                #os.remove(tmppath)
                if spark_sql_exec_result_code != 0:
                    print "spark-sql run sql error,use hive retry"
                    sys.stdout.flush()
                    #如果spark-sql执行失败，降级为hive重新执行
                    hive_exec_result_code = os.system(hive_command_bin + " -f " + tmppath)
                    if hive_exec_result_code != 0:
                        print "user hive retry exec sql error!"
                        sys.stdout.flush()
                        return -1
            return 0
        except Exception, e:
            print(e)
            sys.stdout.flush()
            return 1