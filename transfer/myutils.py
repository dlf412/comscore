#!/usr/bin/env python
# encoding: utf-8
__author__ = 'deng_lingfei'


import datetime
import os
import sys
import config
import time
from functools import partial

app_path = os.path.abspath(os.path.dirname(__file__))
external_lib_path = os.path.join(app_path, '../lib')
sys.path.append(external_lib_path)
from mwlogger import MwLogger
from mwconfig import Mwconfig
from sendmail import send
import ftp_sync
import csv_export
import csv_import

external_etc_file = os.path.join(app_path, '../etc', 'config.json')
json_cfg = Mwconfig(external_etc_file)

now = lambda: int(time.time())


def date_offset_today(n=0):
    today = datetime.date.today()
    day = today + datetime.timedelta(days=n)
    return datetime.datetime.strftime(day, '%Y%m%d')


def notify_customer(subject, body):
    send(subject, body, config.customer_emails, json_cfg.mail_url)


def notify_ourselves(subject, body):
    send(subject, body, config.ourselves_emails, json_cfg.mail_url)


def create_logger(name, handler, level):
    return MwLogger(name, log_handler=handler, log_level=level)


def is_over_time(t):
    now_time = time.strftime('%H:%M:%S', time.localtime())
    return now_time > t


def _notify(mail, daily):
    tos = mail.get('to', [])
    for to in tos:
        send(mail["title"].format(daily=daily),
             mail["content"].format(daily=daily),
             to, json_cfg.mail_url)

filelist_error_notify = partial(_notify, config.filelist_error_mail)
md5_error_notify = partial(_notify, config.md5_error_mail)
success_notify = partial(_notify, config.success_mail)

logger = create_logger(config.log_setting.name,
                       config.log_setting.handler,
                       config.log_setting.level)

if __name__ == '__main__':
    print date_offset_today(-7)
    filelist_error_notify('20160801')
    md5_error_notify('20160801')
    success_notify('20160801')
