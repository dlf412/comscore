#!/usr/bin/env python
# encoding: utf-8

import os
from os.path import join
import traceback
import json
import time
import datetime
from MySQLdb import OperationalError
from functools import partial

from config import *
from sql import *
from include import PAR_DIR
from myamqp import Amqp
from mwlogger import ALogger
from db_txn import db_execute, db_query, db_result, db_txn
from utils import make_pool

logger = ALogger('download_retrier', 'syslog')


def get_newly_date(db_pool):
    def newly_date():
        rc, rs = yield db_query(NEWLY_DATE)
        yield db_result(rc, rs)
    big_dates = ''
    if config_fetch_day <= 0:
        _, end = db_txn(db_pool, partial(newly_date))
        if end:
            start = end[0][0] + datetime.timedelta(config_fetch_day)
            big_dates = "AND url_date >= '{}'".format(start)
    return big_dates


def is_downloading(db_pool, max_file_size, domains, config_dates, big_dates):
    def count(max_file_size, domains, config_dates, big_dates):
        sql = DOWNLOADING_COUNT.format(
            file_size=max_file_size, domains=domains, dates=config_dates,
            big_dates=big_dates)
        # logger.debug('load tasks: {}'.format(sql))
        rc, rs = yield db_query(sql)
        yield db_result(rc, rs)
    bDownloading = True
    _, res = db_txn(db_pool, partial(
        count, max_file_size, domains, config_dates, big_dates))
    if res:
        if res[0][0] > config_max_downloading:
            bDownloading = False
    return bDownloading


def tasks(max_file_size, domains, config_dates, big_dates, max_task_count):
    sql = LOAD_TASKS.format(
        file_size=max_file_size, domains=domains, dates=config_dates,
        limit=max_task_count, big_dates=big_dates)
    # logger.debug('load tasks: {}'.format(sql))
    rc, rs = yield db_query(sql)
    yield db_result(rc, rs)


def up_status(db_pool, url_ids, updated_at):
    def _up_status(ids, updated_at):
        sql = UPDATE_STATUS.format(ids=ids, updated_at=updated_at)
        # logger.debug('update status: {}'.format(sql))
        yield db_execute(sql)
    db_txn(db_pool, partial(_up_status, ','.join(url_ids), updated_at))


def send_mq(mq_info, url_ids, pusher):
    push_info = {}
    push_info['id'] = mq_info.id
    push_info['url'] = mq_info.url
    push_info['domain'] = mq_info.domain
    push_info['csv_file'] = mq_info.csv_file
    push_info['csv_file_number'] = mq_info.csv_file_number
    push_info['filename'] = mq_info.file_name
    push_info['date'] = mq_info.url_date.strftime('%Y%m%d')
    push_info['download_count'] = mq_info.download_count
    pusher.send(push_info, retry=True)
    url_ids.append(str(push_info['id']))
    if log_level == 'INFO':
        logger.info('send task to {}'.format(mq_queue))
    else:
        logger.debug('send task to {}, info={}'.format(mq_queue, push_info))


def main():
    logger.info('download_retrier start')
    with open(join(PAR_DIR, 'etc', 'config.json'), 'r') as f:
        c = json.loads(f.read())
    logger.setLevel(log_level)
    pusher = Amqp(
        c['download_retry_queue'], mq_exchange, mq_queue, mq_routing_key)
    db_pool = make_pool(c['work_db'])
    domains = ''
    if '*' not in config_domains:
        domains = "AND domain IN ('{}')".format("','".join(config_domains))
    url_ids = []
    updated_at = datetime.datetime.utcnow()
    while True:
        try:
            if url_ids:
                up_status(db_pool, url_ids, updated_at)
                del url_ids[:]
            big_dates = get_newly_date(db_pool)
            if is_downloading(db_pool, max_file_size, domains,
                              config_dates, big_dates):
                _, url_task = db_txn(db_pool, partial(
                    tasks, max_file_size, domains, config_dates, big_dates,
                    max_task_count))
                if url_task:
                    updated_at = max(map(lambda x: x.updated_at, url_task))
                    logger.info('max updated_at={}, fetch {} tasks'.format(
                        updated_at, len(url_task)))
                    for r in url_task:
                        logger.message_decorate(url_id=r.id)
                        send_mq(r, url_ids, pusher)
                        logger.message_undecorate()
                    if url_ids:
                        up_status(db_pool, url_ids, updated_at)
                        del url_ids[:]
                else:
                    logger.info('no new task, sleep {} seconds'.format(
                        poll_db_interval))
            else:
                logger.info(
                    'downloading in queue > {}, sleep {} seconds'.format(
                        config_max_downloading, poll_db_interval))
        except OperationalError:
            logger.error('MySQL server Error when loop', exc_info=True)
            time.sleep(1)
        except:
            logger.error('download_retrier_except', exc_info=True)
            os._exit(1)
        else:
            time.sleep(poll_db_interval)


if __name__ == '__main__':
    try:
        main()
    except:
        logger.error("download_retrier_unhandle_except: {}".format(
            traceback.format_exc()))
