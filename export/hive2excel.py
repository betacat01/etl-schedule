# -*- coding:utf-8 -*-
# !/usr/bin/env python

import os
import sys

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from optparse import OptionParser
import xlsxwriter
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import traceback
import pyhs2
import bin.global_constant as global_constant

DATA_SPLIT = "|"

configUtil = global_constant.configUtil

'''
传递参数说明
'''


def option_parser():
    usage = "usage: %prog [options] arg1 arg2"

    parser = OptionParser(usage=usage)

    parser.add_option("-n", "--name", dest="name", action="store", type="string",
                      help="excel name")
    parser.add_option("-s", "--subject", dest="subject", action="store", type="string",
                      help="email subject")
    parser.add_option("-c", "--content", dest="content", action="store", type="string",
                      help="email content")
    parser.add_option("-t", "--tables", dest="tables", action="store", type="string",
                      help="hive table split by comma(,)")
    parser.add_option("-q", "--query_sql", dest="query_sql", action="store", type="string",
                      help="hive query split by semicolon(;)")
    parser.add_option("-o", "--output", dest="output", action="store", type="string",
                      help="output type")
    parser.add_option("-r", "--receivers", dest="receivers", action="store", type="string",
                      help="receiver email split by comma(,)")
    return parser


'''
解析参数
'''


def split_args(options, args):
    name = options.name.strip()
    output_type = options.output.strip()
    receivers = options.receivers.strip()

    if name is None or len(name) == 0:
        raise Exception("excel name none")
    if output_type is None or output_type not in ("attachment", "html"):
        raise Exception("output type wrong. need attachment or html")
    if receivers is None or len(receivers) == 0:
        raise Exception("receivers none")
    receivers_array = receivers.split(",")
    return (name, output_type, receivers_array)


'''
[{}]
'''


def desc_colums(connection, table):
    cursor = connection.cursor()
    cursor.execute("desc " + table)
    rows = cursor.fetch()
    col_list = []
    for row in rows:
        if row[0].startswith("#"):
            break
        if len(row[0]) == 0:
            continue
        col_list.append({"col_name": row[0], "col_comment": row[2]})
    cursor.close()
    return col_list


def hive_connection(db):
    host = configUtil.get("hive.host")
    port = configUtil.get("hive.port")
    username = configUtil.get("hive.username")
    password = configUtil.get("hive.password")
    connection = pyhs2.connect(host=host,
                               port=int(port),
                               authMechanism="PLAIN",
                               user=username,
                               password=password,
                               database=db)
    return connection


'''
{"table_name":{"columns:":[comment1,comment2],"data":[]}}
'''

#向HiveSever2发送查询sql，返回查询结果以及schema信息
def query_data(sql,db_name=None,table_name=None):
    query_result_data = []
    query_result_schema = []

    connection = hive_connection(db_name)
    cursor = connection.cursor()
    cursor.execute(sql)

    #取得查询结果的列名称
    if table_name is None:
        columns = cursor.getSchema()
        for column in columns:
            column_info ={}
            col_name = column['columnName']
            col_comment = column['comment']
            if col_comment is None:
                col_comment = col_name
            column_info['col_name'] = col_name
            column_info['col_comment'] = col_comment
            query_result_schema.append(column_info)
    else:
        query_result_schema = desc_colums(connection,table_name)

    #取得查询结果的数据
    rows = cursor.fetch()
    for row in rows:
        row_data_list = []
        for col in row:
            row_data_list.append(str(col))
        query_result_data.append(row_data_list)

    return (query_result_schema,query_result_data)





def query_table(name, tables):
    excel_path_dir = configUtil.get("tmp.path") + "/excel"
    if not os.path.exists(excel_path_dir):
        os.makedirs(excel_path_dir)
    excel_path = excel_path_dir + "/" + name + ".xlsx"
    if os.path.exists(excel_path):
        os.remove(excel_path)
    workbook = xlsxwriter.Workbook(excel_path)
    for sheet, table in enumerate(tables):
        db_name = table.split(".")[0]
        table_name = table.split(".")[1]
        connection = hive_connection(db_name)
        col_list = desc_colums(connection, table)
        # print col_list
        cursor = connection.cursor()
        col_select_name = []
        col_show_name = []
        for col in col_list:
            col_select_name.append(col["col_name"])
            col_show_name.append(col["col_comment"])
        sql = "select " + ",".join(col_select_name) + " from " + table
        print("sql:" + sql)
        cursor.execute(sql)
        rows = cursor.fetch()
        list_data = []
        for row in rows:
            data = []
            for index, val in enumerate(col_select_name):
                data.append(str(row[index]))
            list_data.append(DATA_SPLIT.join(data))
        # print list_data
        # print cursor.getSchema()
        # write2file(",".join(col_show_name),list_data)
        sheet_name = "工作表" + str(sheet + 1)
        write2excel(workbook, sheet_name, col_show_name, list_data)
    workbook.close()
    return excel_path

#将查询结果写入到excel文件
def convert_query_result_to_excel(query_result):
    excel_path_dir = configUtil.get("tmp.path") + "/excel"
    if not os.path.exists(excel_path_dir):
        os.makedirs(excel_path_dir)
    excel_path = excel_path_dir + "/" + name + ".xlsx"
    if os.path.exists(excel_path):
        os.remove(excel_path)
    workbook = xlsxwriter.Workbook(excel_path)
    sheet = 0
    for (query_result_schema, query_result_data) in query_result:

        col_show_name = []
        for col in query_result_schema:
            col_show_name.append(col["col_comment"])

        list_data = []
        for row in query_result_data:
            list_data.append(DATA_SPLIT.join(row))

        sheet += 1
        sheet_name = "工作表" + str(sheet)
        write2excel(workbook, sheet_name, col_show_name, list_data)

    workbook.close()
    return excel_path




def get_html_content(tables):
    html_content = "<br>"
    for table in tables:
        db_name = table.split(".")[0]
        connection = hive_connection(db_name)
        col_list = desc_colums(connection, table)
        # print col_list
        cursor = connection.cursor()
        col_select_name = []
        col_show_name = []
        for col in col_list:
            col_select_name.append(col["col_name"])
            col_show_name.append(col["col_comment"])
        html_content += "<table border='1'><tr><th>" + "</th><th>".join(col_show_name) + "</th></tr>"
        sql = "select " + ",".join(col_select_name) + " from " + table
        print("sql:" + sql)
        cursor.execute(sql)
        rows = cursor.fetch()
        for row in rows:
            html_content += "<tr><td>" + "</td><td>".join([str(elem) for elem in row]) + "</td></tr>"
        html_content += "</table><br>"
    return html_content

#将查询结果转换为html中table的格式
def convert_query_result_to_html_content(query_result):
    html_content = "<br>"

    for (query_result_schema, query_result_data) in query_result:
        col_show_name = []
        for col in query_result_schema:
            col_show_name.append(col["col_comment"])
        html_content += "<table border='1'><tr><th>" + "</th><th>".join(col_show_name) + "</th></tr>"
        for row in query_result_data:
            html_content += "<tr><td>" + "</td><td>".join(row) + "</td></tr>"
        html_content += "</table><br>"
    return html_content



def is_float(data):
    try:
        float(data)
        return True
    except Exception, e:
        return False


def is_int(data):
    try:
        int(data)
        return True
    except Exception, e:
        return False


'''
写入数据到excel
'''


def write2excel(workbook, sheet_name, head, data):
    worksheet = workbook.add_worksheet(sheet_name)
    # write head
    for index, head_str in enumerate(head):
        worksheet.write_string(0, index, head_str)
    # write data
    for index, data_str in enumerate(data):
        for data_col_index, data_col in enumerate(data_str.split(DATA_SPLIT)):
            if str(data_col).upper() == "INF" or str(data_col).upper() =="NAN" :
                worksheet.write_string(index + 1, data_col_index, data_col)
            elif is_int(data_col):
                worksheet.write_number(index + 1, data_col_index, (int)(data_col))
            elif is_float(data_col):
                worksheet.write_number(index + 1, data_col_index, (float)(data_col))
            else:
                worksheet.write_string(index + 1, data_col_index, data_col)


'''
 发送邮件
'''


def send_email(subject, content, receivers_array, excel_path = None):
    contype = 'application/octet-stream'
    maintype, subtype = contype.split('/', 1)
    server = smtplib.SMTP_SSL(host=configUtil.get("email.host"),port=configUtil.get("email.port"))
    server.login(configUtil.get("email.username"), configUtil.get("email.password"))
    main_msg = MIMEMultipart()
    text_msg = MIMEText(content, _subtype='html', _charset='utf-8')
    # email text
    main_msg.attach(text_msg)
    #  email attach
    if excel_path is not None:
        print "excel path:" + str(excel_path)
        data = open(excel_path, 'rb')
        attach = MIMEText(data.read(), 'base64', 'gb2312')
        basename = os.path.basename(excel_path)
        attach["Content-Type"] = 'application/octet-stream'
        attach.add_header('Content-Disposition', 'attachment', filename=basename.encode("gb2312"))
        main_msg.attach(attach)
    main_msg["Accept-Language"] = "zh-CN"
    main_msg["Accept-Charset"] = "utf-8"
    main_msg['From'] = configUtil.get("email.username")
    main_msg['To'] = ",".join(receivers_array)
    main_msg['Subject'] = subject
    full_text = main_msg.as_string()
    server.sendmail(configUtil.get("email.username"), receivers_array, full_text)
    server.quit()

'''
 for test
'''


def write2file(head, data):
    file_handler = open("/home/hadoop/yangxl/script/excel.csv", 'w')
    file_handler.write(head + '\n')
    file_handler.flush()
    for line in data:
        file_handler.write(line + '\n')
    file_handler.flush()
    file_handler.close()


if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding('utf-8')

    # fakeArgs = ["-s","财务月度数据" ,"-c","详细数据见附件","-n","财务","-t", "tmp.tmp_bid_task_agg_month,tmp.tmp_bid_task_month", '-o', "attachment", '-r', "yangxiaolong@yunniao.me"]

    optParser = option_parser()

    # options, args = optParser.parse_args(fakeArgs)
    options, args = optParser.parse_args(sys.argv[1:])

    print options

    if options.subject is None:
        print("require email subject")
        optParser.print_help()
        sys.exit(-1)
    if options.content is None:
        print("require email content")
        optParser.print_help()
        sys.exit(-1)
    if options.name is None:
        print("require excel name")
        optParser.print_help()
        sys.exit(-1)
    #tables和query_sql至多包含一个
    if (options.tables is None and options.query_sql is None) or (options.tables is not None and options.query_sql is not None):
        print("tables 和 query_sql不能同时存在，只能包含其中的一个")
        optParser.print_help()
        sys.exit(-1)

    if options.output is None:
        print("require output type")
        optParser.print_help()
        sys.exit(-1)
    if options.receivers is None:
        print("require receiver split  by comma")
        optParser.print_help()
        sys.exit(-1)

    try:
        (name, output_type, receivers_array) = split_args(options, args)

        if configUtil.getBooleanOrElse("send.email", True):

            query_result = []

            #1.查询数据
            if options.query_sql is not None: #1.1按照指定的自定义sql来查询数据
                sql_array = options.query_sql.split(";")
                for sql in sql_array:
                    (query_result_schema, query_result_data) = query_data(sql)
                    query_result.append((query_result_schema, query_result_data))
            elif options.tables is not None:#1.2按照指定的表名来动态的拼装sql，然后查询数据
                tables = options.tables.split(",")
                for table in tables:
                    db_name = table.split(".")[0]
                    table_name = table.split(".")[1]
                    sql = "select * from " + table
                    (query_result_schema, query_result_data) = query_data(sql,db_name,table_name)
                    query_result.append((query_result_schema, query_result_data))



            #2.根据不同的output_type来将查询结果保存到不同的地方
            email_content = options.content.strip()
            excel_attach_path = None
            if output_type == "attachment":
                excel_attach_path = convert_query_result_to_excel(query_result)
            elif output_type == "html":
                html_content = convert_query_result_to_html_content(query_result)
                email_content = email_content + html_content


            #3.发送邮件
            send_email(options.subject.strip(), email_content, receivers_array, excel_attach_path)

        else:
            print("不需要发送邮件")

        # if configUtil.getBooleanOrElse("send.email", True):
        #     if output_type == "attachment":
        #         excel_path = query_table(name, tables)
        #         email_content = options.content.strip()
        #         send_email(options.subject.strip(), email_content, receivers_array, excel_path)
        #     elif output_type == "html":
        #         email_content = options.content.strip() + get_html_content(tables)
        #         send_email(options.subject.strip(), email_content, receivers_array)
        # else:
        #     print("不需要发送邮件")

    except Exception, e:
        print traceback.format_exc()
        sys.exit(-1)