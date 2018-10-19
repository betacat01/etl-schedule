# -*- coding:utf-8 -*-
#全局常量定义
import os
from common_util.configutil import ConfigUtil
from common_util.dateutil import DateUtil
from common_util.dbutil import DBUtil
from common_util.smsutil import SMSUtil

project_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
configUtil = ConfigUtil(project_path)
configUtil
dateUtil = DateUtil
smsUtil = SMSUtil(configUtil)


