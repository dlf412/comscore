#!/usr/bin/env python
# encoding: utf-8
from __future__ import division
import commands
import sys
from os.path import join
import traceback
import json
import datetime

from config import (config_down_ftp, config_upload_ftp,
                    config_mycompany_email, config_comscore_email)
from include import PAR_DIR, logger
from db_access import AccessDB
from ftp_sync import FTPSync
from reference_logic import process
from ingestor_email import EmailDeliver


def run(input_date, down_ftp, upload_ftp, db_conn, config, email):
    if db_conn.is_referenceDay_exists(input_date) and \
            not db_conn.is_referenceDay_lock(input_date):
        print u'ERROR ! 表referenceDay在{}锁定标志'\
            u'status为true，该天任务已经处理完毕，无法继续！'.format(input_date)
    else:
        try:
            ret = process(input_date, down_ftp, upload_ftp, db_conn, config,
                          email, True)
            if ret:
                print u'处理成功，csv已经上传到ftp，通知邮件正常发送'
            else:
                print u'处理失败，请查看日志'
        except:
            print traceback.format_exc()


def is_error_parameter():
    if len(sys.argv) <= 1:
        print u'手动母片入库，需要带日期参数，例如 20160909'
        return True
    try:
        datetime.datetime.strptime(sys.argv[1], '%Y%m%d')
    except:
        print u'参数格式错误，需要格式如20160808'
        return True
    ret, pro_test = commands.getstatusoutput('ps aux | grep "ingestor.py"')
    if pro_test.count('manual_ingestor.py') > 1:
        print u'需要先停止另外的母片入库程序'
        return True
    if pro_test.count('/ingestor.py') > 0:
        print u'需要先停止另外的母片入库程序'
        return True


def main():
    if is_error_parameter():
        return
    print 'manual ingestor start'
    input_date = sys.argv[1]
    config = json.load(open(join(PAR_DIR, 'etc', 'config.json'), 'r'))
    assert('redis' in config)
    db_conn = AccessDB(config['work_db'])
    down_ftp = FTPSync(config_down_ftp['host'], config_down_ftp['user'],
                       config_down_ftp['passwd'])
    upload_ftp = FTPSync(config_upload_ftp['host'], config_upload_ftp['user'],
                         config_upload_ftp['passwd'])
    email = EmailDeliver(config_comscore_email, config_mycompany_email)
    run(input_date, down_ftp, upload_ftp, db_conn, config, email)


if __name__ == '__main__':
    try:
        main()
    except:
        logger.error('ingestor_unhandle_except', exc_info=True)
