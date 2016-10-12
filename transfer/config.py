#!/usr/bin/env python
# encoding: utf-8
__author__ = 'deng_lingfei'

'''
the transfer.py's configures
sftp访问地址、用户与密码
正查时间范围
结果交付时间
本地目录（和ftp目录一致）
客户邮箱地址
告警邮箱地址
告警邮件标题与内容
log_level
log_name
log_handler
'''


class ftp_setting(object):
    host = 'ftp.comscore.com'
    user = 'Vobile'
    password = 'k49d77h3'

local_dir = './'

# send mail to customer if dailyInput files be not ready over the time
notify_customer_time = "11:00:00"

results_delivered_agos = 48  # 交付N天前的结果
results_delivered_time = "08:00:00"  # 交付结果的时间

customer_emails = ['deng_lingfei@vobile.cn']
ourselves_emails = ['deng_lingfei@vobile.cn']


filelist_error_mail = {
    "title": "[Error]Unable to Find Filelist.txt for Ad URL CSV File {daily}",
    "content": """Hi,

We are unable to detect the “filelist.txt” for Ad URL CSV file of
{daily} within the predefined time frame.

Please upload the CSV file and the text file to the correct directory.

Yours sincerely,
Vobile MediaWise Team""",
    "to": [customer_emails, ourselves_emails]
}


md5_error_mail = {
    "title": "[Error]Filelist中记录的CSV文件MD5不正确 {daily}",
    "content": """
项目：comScore
文件日期：{daily}
问题：comScore当日上传的CSV文件的MD5值与Filelist中记录的不一致，请人
工确认不一致后联系客户重新上传。""",
    "to": [ourselves_emails]
}


success_mail = {
    "title": "[Success]Ad URL Match File Has Been Uploaded {daily}",
    "content": """Hi,

We have successfully processed the Ad URL file for {daily} and
have uploaded the match result CSV file to your FTP.

Please login to the FTP to retrieve the match result file.

Yours sincerely,
Vobile MediaWise Team""",
    "to": [customer_emails, ourselves_emails]
}


class log_setting(object):
    level = 'INFO'
    name = 'transfer'
    handler = 'syslog'
