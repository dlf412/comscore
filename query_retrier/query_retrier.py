#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
   Copyright (c) 2016 Vobile Inc.  All Rights Reserved.
   Author: xu_xiaorong
   Email: xu_xiaorong@vobile.cn
   Created_at: 2016-08-16 09:12:24
'''

import os
import sys
import time
import commands
import traceback
from functools import partial
from os import makedirs
from collections import defaultdict
from xml.etree.ElementTree import fromstring, Element, parse, tostring
from os.path import dirname, abspath, exists
from os.path import join as path_join

from conf import *

WORK_DIR = dirname(dirname(abspath(__file__)))
sys.path.append(path_join(WORK_DIR, 'lib'))


from mwconfig import Mwconfig
from myamqp import Amqp
from mwlogger import ALogger
from utils import make_pool
from db_txn import db_txn, db_query, db_execute, db_result

g_logger = ALogger("query_retrier", log_handler=LOG_HANDLER,
                   log_level=LOG_LEVEL)


def fetch_tasks(pool, active_time, count):
    def _t(active_time, count):
        sql = '''select id, date_format(url_date, '%%Y%%m%%d') as url_date,
                        csv_file, csv_file_number, video_path,
                        feature_dir, file_md5
                   from url
                  where status='query_failed'
                        and timestampdiff(second, created_at, now()) < %s
                  limit %s'''
        rc, rs = yield db_query(sql, active_time, count)
        yield db_result(rc, rs)
    return db_txn(pool, partial(_t, active_time, count))


def set_task_query(pool, url_id):
    def _t(url_id):
        sql = '''update url set status='query' where id=%s'''
        rc, _ = yield db_execute(sql, url_id)
        yield db_result(rc)
    return db_txn(pool, partial(_t, url_id))


def run(db_url, queue_url):
    pool = make_pool(db_url)
    while True:
        try:
            _, tasks = fetch_tasks(pool, TASK_ACTIVE_TIME, FETCH_COUNT)
            g_logger.debug("retry tasks: %s", tasks)
            with Amqp(queue_url, QUEUE_EXCHANGE, QUEUE_NAME, QUEUE_ROUTING_KEY) as q:
                for t in tasks:
                    q.send(t._asdict())
                    g_logger.info("add to retry queue, url_id: %s", t.id)
                    set_task_query(pool, t.id)
                    g_logger.info("update status query, url_id: %s", t.id)
            g_logger.info("wait %s seconds for next fetch", FETCH_INTERVAL)
            time.sleep(FETCH_INTERVAL)
        except:
            g_logger.error("unhandle error", exc_info=True)
            time.sleep(1)


def main():
    conf = Mwconfig(path_join(WORK_DIR, "etc", "config.json"))
    queue_url = conf.query_retry_queue
    db_url = conf.work_db
    g_logger.info("start query_retrier")
    run(db_url, queue_url)

if __name__ == '__main__':
    main()
