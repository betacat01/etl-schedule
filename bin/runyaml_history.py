# -*- coding:utf-8 -*-
'''
用于重跑历史上一段时间区间的yaml任务
'''
import global_constant
from runyaml import  RunYaml
from optparse import OptionParser
import  sys


def option_parser():
    usage = "usage: %prog [options]"

    parser = OptionParser(usage=usage)

    parser.add_option("-s", "--start", dest="start", action="store", type="string",
                      help="start date yyyy-MM-dd")
    parser.add_option("-e", "--end", dest="end", action="store", type="string",
                      help="end date yyyy-MM-dd")
    parser.add_option("-p", "--path", dest="path", action="store", type="string",
                      help="yaml file path")

    return parser


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')
    optionsParser = option_parser()
    options, args = optionsParser.parse_args(sys.argv[1:])

    if options.path is None:
        print("require yaml file")
        optionsParser.print_help()
        sys.exit(-1)

    run_yaml = RunYaml()

    start = options.start
    end = options.end

    if start is None:
        start = global_constant.dateUtil.get_now()
    else:
        start = global_constant.dateUtil.parse_date(start)

    if end is None:
        end = global_constant.dateUtil.get_now()
    else:
        end = global_constant.dateUtil.parse_date(end)

    run_code = []
    for p_day in global_constant.dateUtil.get_list_day(start, end):
        print "运行时设置的日期:", p_day
        code = run_yaml.run_command(options.path, p_day)
        run_code.append(str(code))
        if code != 0:
            sys.exit(1)
    code_str = ",".join(run_code)
    print "运行结果:", code_str


