#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
   Copyright (c) 2016 Vobile Inc.  All Rights Reserved.
   Author: xu_xiaorong
   Email: xu_xiaorong@vobile.cn
   Created_at: 2016-08-12 11:30:18
'''

import sys

from functools import partial
from query import *
from conf import *


WORK_DIR = dirname(dirname(abspath(__file__)))
sys.path.append(path_join(WORK_DIR, 'lib'))

from mwconfig import Mwconfig
from myamqp import Amqp
from db_txn import db_txn, db_execute, db_query, db_result

config = Mwconfig(path_join(WORK_DIR, "etc", "config.json"))
from utils import make_pool
pool = make_pool(config['work_db'])

def insert_task(task):
    def _t(task):
        sql = '''
                insert into url (id, ad_idc, file_name, url, url_date, domain,
                            file_md5, csv_file, csv_file_number, video_path,
                            status)
                       values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                       'query')
              '''
        yield db_execute(sql, task["id"], task["ad_idc"], task["file_name"],
                         task["url"], task["url_date"], task["domain"],
                         task["file_md5"], task["csv_file"],
                         task["csv_file_number"],
                         task["video_path"])
    return db_txn(pool, partial(_t, task))

task = {
        "id": 1024,
        "ad_idc": "ad_idc",
        "file_name":"5ddae24955ea38dde6aed78b34368d6b.f4v",
        'url_date': "20160801",
        "url": "www.url.com/xxx/yyy.flv",
        "domain": "url.com",
        "file_md5": "5ddae24955ea38dde6aed78b34368d6b",
        "csv_file": "csv", "csv_file_number": 1,
        "video_path":
        "/home/media_wise/5ddae24955ea38dde6aed78b34368d6b.f4v"
        }

def test_add_normal_task():
    insert_task(task)
    mq = Amqp(config["query_queue"],
              QUEUE_EXCHANGE,
              QUEUE_NAME,
              QUEUE_ROUTING_KEY)
    mq.send(task, retry=True)


test_add_normal_task()

def test_add_retry_task():
    pass



