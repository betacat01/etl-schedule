#!/usr/bin/python
# -*- coding:utf-8 -*-

import os
import traceback
import sys
import global_constant
from runcommand import RunCommand
from optparse import OptionParser

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_path)



class RunYaml(object):
    def __init__(self):
        self.config = global_constant.configUtil
        pass

    def option_parser(self):
        usage = "usage: %prog [options] arg1"

        parser = OptionParser(usage=usage)

        parser.add_option("-d", "--run_date", dest="run_date", action="store", type="string",
                          help="run date yyyy-MM-dd")
        parser.add_option("-p", "--path", dest="path", action="store", type="string",
                          help="yaml file path")

        parser.add_option("--date_format", dest="date_format", action="store", default='%Y-%m-%d', type="string",
                          help="run_date参数的日期格式化参数，默认%Y-%m-%d")

        return parser

    def run_command(self, path, p_day, fmt='%Y-%m-%d'):
        try:
            print("脚本位置:" + path)

            if not os.path.exists(path):
                raise Exception("can't find run script:" + path)

            extend = os.path.splitext(path)[1]
            if extend == ".yml":
                run_command = RunCommand()
                code = run_command.run_yaml(path, p_day, fmt)
                return code
            else:
                raise Exception("当前只支持 yml 脚本")
        except Exception, e:
            print("Executor 上 job 运行失败:" + path)
            print(traceback.format_exc())
            return 1


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    run_yaml = RunYaml()
    optParser = run_yaml.option_parser()
    options, args = optParser.parse_args(sys.argv[1:])

    if options.path is None:
        print("require yaml file")
        optParser.print_help()
        sys.exit(-1)

    run_date = options.run_date

    if run_date is None:
        run_date = global_constant.dateUtil.get_now_fmt(fmt=options.date_format)

    code = run_yaml.run_command(options.path, run_date, fmt=options.date_format)

    print options.date_format
    print run_date
    if code != 0:
        sys.exit(1)
    print "运行结果:", code
