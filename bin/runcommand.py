#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import sys

import subprocess
import global_constant
from yamlparser import YamlParser
import yaml
import traceback

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)
from export.HiveUtil import HiveUtil
from export.SparkSqlUtil import SparkSqlUtil

'''
用来运行具体的脚本
'''


class RunCommand(object):
    def __init__(self):
        self.config = global_constant.configUtil


    def get_python_bin(self):
        python_path = self.config.get("python.home")
        if python_path is None or len(python_path) == 0:
            raise Exception("can't find python.home")
        python_bin = python_path + "/bin/python"
        return python_bin


    def run_single_command(self, command):
        print "start run command: " + " ".join(command)
        child = subprocess.Popen(command,
                                 stdout=None,
                                 stderr=subprocess.STDOUT,
                                 shell=False)
        code = child.wait()
        return code

    def run_yaml(self, yaml_file, init_day=None, fmt='%Y-%m-%d'):
        yaml_sql_path = self.config.get("job.script.path") + "/sql"
        yaml_parser = YamlParser()
        yaml_file = open(yaml_file, 'r')
        yaml_dict = yaml.safe_load(yaml_file)
        steps = yaml_dict['steps']
        if steps and len(steps) > 0:
            for step in steps:
                step_type = step['type']
                if step_type == 'hive' or step_type == 'spark_sql':
                    (vars, sqls, sql_paths) = yaml_parser.parse_hive_sql(step, init_day, fmt)
                    if sqls or sql_paths:
                        if step_type == 'hive':
                            query_engine_util = HiveUtil()
                        elif step_type == 'spark_sql':
                            query_engine_util = SparkSqlUtil()
                        query_engine_util.add_vars(vars)
                        query_engine_util.add_sqls(sqls)
                        query_engine_util.add_sql_paths(yaml_sql_path, sql_paths)
                        code = query_engine_util.run_sql_by_client()
                        if code != 0:
                            return 1
                if step_type == 'export':
                    command_list = yaml_parser.parse_export(self.get_python_bin(), project_path, step, init_day, fmt)
                    if command_list and len(command_list) > 0:
                        for command in command_list:
                            code = self.run_single_command(command)
                            if code != 0:
                                return 1
            return 0
        else:
            return 0


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
