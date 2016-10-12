#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
   Copyright (c) 2016 Vobile Inc.  All Rights Reserved.
   Author: xu_xiaorong
   Email: xu_xiaorong@mycompany.cn
   Created_at: 2016-08-10 13:46:22
'''
import os
import sys
from functools import partial
from os.path import dirname, abspath, exists
from os.path import join as path_join

from conf import *

WORK_DIR = dirname(dirname(abspath(__file__)))
sys.path.append(path_join(WORK_DIR, 'lib'))
DATA_DIR = path_join(WORK_DIR, "var", "video")
VIDEO_SEARCH = path_join(WORK_DIR, 'tools', 'video_search')


from myamqp import Amqp
from utils import make_pool
from mwlogger import ALogger
from mwconfig import Mwconfig
from db_txn import db_txn, db_query, db_execute, db_result
from query_utils import create_dna, query, CreateDnaError, QueryDnaError


g_logger = ALogger("querier", log_handler=LOG_HANDLER, log_level=LOG_LEVEL)


def same_task_finished(pool, file_md5):
    def _t(file_md5):
        sql = '''select id, feature_dir, match_meta
                   from url
                  where status='query_success' and file_md5=%s
                    and match_meta IS NOT NULL'''
        rc, rs = yield db_query(sql, file_md5)
        yield db_result(rc, rs)
    return db_txn(pool, partial(_t, file_md5))


def as_finish(pool, task, brother):
    def _t(task, brother):
        sql1 = '''update url
                     set status='query_success', feature_dir=%s,
                         match_meta=%s, is_valid_file='true'
                   where id=%s'''
        rc, _ = yield db_execute(sql1, brother['feature_dir'],
                                 brother['match_meta'], task['id'])
        yield db_result(rc)
    return db_txn(pool, partial(_t, task, brother))


def task_finished(pool, task, meta):
    def _t(task, meta):
        sql = '''update url
                    set status='query_success', feature_dir=%s,
                        match_meta=%s, query_count=query_count+1,
                        is_valid_file='true'
                  where id=%s'''
        rc, _ = yield db_execute(sql, task['feature_dir'], meta, task['id'])
        yield db_result(rc)
    return db_txn(pool, partial(_t, task, meta))


def task_failed(pool, task, valid=True):
    def _t(task, valid):
        sql = '''update url
                    set status='query_failed', is_valid_file=%s,
                        query_count=query_count+1, feature_dir=%s
                  where id=%s'''
        rc, _ = yield db_execute(sql, str(valid).lower(),
                                 task.get('feature_dir', ''),
                                 task['id'])
        yield db_result(rc)
    return db_txn(pool, partial(_t, task, valid))


def _errback(exc_info, interval):
    g_logger.error("rabbitmq gone away, reconnect after %s seconds",
                   interval, exc_info=True)


def run(queue_url, db_url, redis):
    def process_task(task, message):
        try:
            g_logger.info("received query task: %s", task)
            g_logger.message_decorate(url_id=task.get("id", None))
            rc, rs = same_task_finished(pool, task['file_md5'])
            if rc:
                g_logger.info("find md5 %s with matches", task['file_md5'])
                as_finish(pool, task, rs[0]._asdict())
                g_logger.info("task finished")
            else:
                g_logger.info("no same task in db")
                if not task.get("feature_dir", ""):
                    task["feature_dir"] = path_join(WORK_DIR, 'var', 'video',
                                                    task['url_date'], task[
                                                        'csv_file'],
                                                    str(task[
                                                        'csv_file_number'] / 10000),
                                                    str(task['id']))
                # if `key` and `frame` already exists,
                # `create_dna` will return directly
                key, frame = create_dna(VIDEO_SEARCH, task["video_path"],
                                        task["file_md5"], task["feature_dir"])
                g_logger.info("gen feature success, feature_dir: %s",
                               task["feature_dir"])
                meta = query(VIDEO_SEARCH, task["video_path"], key, frame,
                             redis["host"], redis["port"], redis["password"],
                             match_threshold=MATCH_THRESHOLD)
                g_logger.info("query finished, matches: %s", meta)
                task_finished(pool, task, meta)
                g_logger.info("task finished")
        except CreateDnaError as e:
            g_logger.error("invalid file, error: %s", e, exc_info=True)
            task_failed(pool, task, valid=False)
        except QueryDnaError as e:
            g_logger.error("query error, error: %s", e, exc_info=True)
            task_failed(pool, task)
        except:
            g_logger.error("unhandle error, task: %s", task, exc_info=True)
            task_failed(pool, task)
        finally:
            g_logger.message_undecorate()
        message.ack()
    mq = Amqp(queue_url,
              QUEUE_EXCHANGE,
              QUEUE_NAME,
              QUEUE_ROUTING_KEY)
    pool = make_pool(db_url)
    g_logger.info("start query service")
    mq.poll(process_task, errback=_errback)  # ignore exception

if __name__ == '__main__':
    config = Mwconfig(path_join(WORK_DIR, "etc", "config.json"))
    run(config.query_queue, config.work_db, config.redis)
