#!/usr/bin/env python
# encoding: utf-8

import os
from os.path import join
import traceback
import json
import time
from MySQLdb import OperationalError
from functools import partial

from config import *
from sql import *
from include import PAR_DIR
from myamqp import Amqp
from mwlogger import ALogger
from db_txn import db_execute, db_query, db_result, db_txn
from utils import make_pool

logger = ALogger('distributor', 'syslog')


def tasks(domains, config_dates, max_task_count):
    sql = LOAD_TASKS.format(
        domains=domains, dates=config_dates, limit=max_task_count)
    # logger.debug(sql)
    rc, rs = yield db_query(sql)
    yield db_result(rc, rs)


def up_status(db_pool, url_ids):
    def _up_status(id):
        sql = UPDATE_STATUS.format(ids=id)
        # logger.debug(sql)
        yield db_execute(sql)
    db_txn(db_pool, partial(_up_status, ','.join(url_ids)))


def send_mq(mq_info, url_ids, pusher):
    push_info = {}
    push_info['id'] = mq_info.id
    push_info['url'] = mq_info.url
    push_info['domain'] = mq_info.domain
    push_info['csv_file'] = mq_info.csv_file
    push_info['csv_file_number'] = mq_info.csv_file_number
    push_info['filename'] = mq_info.file_name
    push_info['date'] = mq_info.url_date.strftime('%Y%m%d')
    push_info['download_count'] = 0
    pusher.send(push_info, retry=True)
    url_ids.append(str(push_info['id']))
    if log_level == 'INFO':
        logger.info('send task to {}'.format(mq_queue))
    else:
        logger.debug('send task to {},info={}'.format(mq_queue, push_info))


def main():
    try:
        logger.info('distributor start')
        with open(join(PAR_DIR, 'etc', 'config.json'), 'r') as f:
            c = json.loads(f.read())
        logger.setLevel(log_level)
        pusher = Amqp(
            c['download_queue'], mq_exchange, mq_queue, mq_routing_key)
        db_pool = make_pool(c['work_db'])
        domains = ''
        if '*' not in config_domains:
            domains = "AND domain IN ('{}')".format("','".join(config_domains))
        url_ids = []
        while True:
            try:
                if url_ids:
                    up_status(db_pool, url_ids)
                    del url_ids[:]
                _, url_task = db_txn(db_pool, partial(
                    tasks, domains, config_dates, max_task_count))
                if url_task:
                    logger.info('fetch {} tasks'.format(len(url_task)))
                else:
                    logger.info('no new task, sleep {} seconds'.format(
                        poll_db_interval))
                for r in url_task:
                    logger.message_decorate(url_id=r.id)
                    send_mq(r, url_ids, pusher)
                    logger.message_undecorate()
                if url_ids:
                    up_status(db_pool, url_ids)
                    del url_ids[:]

            except OperationalError:
                logger.error('MySQL server Error when loop', exc_info=True)
                time.sleep(1)
            except:
                logger.error('distributor_except when loop', exc_info=True)
                os._exit(1)
            else:
                time.sleep(poll_db_interval)
    except:
        logger.error("distributor_unhandle_except: {}".format(
            traceback.format_exc()))


if __name__ == '__main__':
    main()
