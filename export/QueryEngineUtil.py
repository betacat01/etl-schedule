#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import sys
from abc import abstractmethod

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import bin.global_constant as global_constant


class QueryEngineUtil:
    def __init__(self):
        self.config = global_constant.configUtil
        self.sqls = []
        self.vars = []

    def add_sql(self, sql):
        self.sqls.append(sql)

    def add_sqls(self, sqls):
        for sql in sqls:
            self.sqls.append(sql)

    def add_var(self, var):
        self.vars.append(var)

    def add_vars(self, vars):
        for var in vars:
            self.add_var(var)

    def add_sql_paths(self,base_path,sql_paths):
        for sql_path in sql_paths:
            self.add_sql_path(base_path, sql_path)

    def add_sql_path(self,base_path,sql_path):
        if base_path and len(base_path) > 0:
            sql_path = base_path + "/" + sql_path
        sql_handler = open(sql_path,'r')
        sqls = ""
        for line in sql_handler.readlines():
            sqls += line
        self.add_sqls((sqls,))
        sql_handler.flush()
        sql_handler.close()

    @abstractmethod
    def get_connection(self):
        pass

    def run_sql_count(self, sql):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            print "start run sql count:" + str(sql)
            sql = sql.replace(";", "")
            cursor.execute(sql)
            count = 0
            for i in cursor.fetch():
                count = i[0]
            connection.close()
            return count
        except Exception, e:
            print(e)
            connection.close()
            return 0

    def run_sql_by_connection(self, sql):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            print "start run sql:" + str(sql)
            sql = sql.replace(";", "")
            cursor.execute(sql)
            connection.close()
            return 0
        except Exception, e:
            print(e)
            connection.close()
            return -1

    def run_all_sqls_by_connection(self):
        try:
            connection = self.get_connection()
            cursor = connection.cursor()
            for sql in self.sqls:
                print "start run sql:" + str(sql)
                sql = sql.replace(";", "")
                cursor.execute(sql)
            connection.close()
            return 0
        except Exception, e:
            print(e)
            connection.close()
            return -1

    @abstractmethod
    def run_sql_by_client(self):
        pass


    def check_run_code(self, code=-1):
        if code == 0:
            sys.exit(0)
        else:
            sys.exit(1)
