#!/usr/bin/env python
# encoding: utf-8

import sys
import os
from os.path import join
import traceback
import json
import time
from MySQLdb import OperationalError
from functools import partial
import commands
from pipes import quote
import re

from config import *
from sql import *
from include import PAR_DIR
from myamqp import Amqp
from mwlogger import ALogger
from db_txn import db_execute, db_query, db_result, db_txn
from utils import make_pool, trans2utf8

g_bDownloader_re = False
if len(sys.argv) > 1:
    logger = ALogger('downloader_re', 'syslog')
    g_bDownloader_re = True
else:
    logger = ALogger('downloader', 'syslog')


def download(task):
    def make_dirs():
        file_par_path = join(
            PAR_DIR, 'var', 'video', task['date'], task['csv_file'],
            str(int(int(task['csv_file_number']) / 10000)), str(task['id']))
        if not os.path.exists(file_par_path):
            # logger.debug('makedirs: {}'.format(file_par_path))
            os.makedirs(file_par_path)
        return (file_par_path, join(file_par_path, task['filename']),
                join(file_par_path, 'log'))

    def wget_download():
        logger.debug('start wget,url={},save_file={}'.format(url, file_path))
        status, output = commands.getstatusoutput(WGET_COMMAND.format(
            url=quote(url), file=quote(file_path), log=quote(log_path)))
        return status, output

    def get_file_size():
        if os.access(file_path, os.R_OK):
            return os.stat(file_path).st_size
        return 0

    def get_file_md5():
        md5sum_ret = commands.getoutput("md5sum {}".format(
            quote(file_path))).strip()
        return md5sum_ret.split()[0]

    def get_down_speed():
        try:
            down_speed = int((file_size / 1024) / (time2 - time1))
        except:
            down_speed = 0
        return down_speed

    def rename_file_name():
        new_file_path = join(file_par_path, file_md5)
        if task['filename'] and '.' in task['filename']:
            ext = task['filename'].split('.')[-1]
            if ext:
                new_file_path = '{}.{}'.format(new_file_path, ext)
        try:
            os.rename(file_path, new_file_path)
        except:
            new_file_path = file_path
        return new_file_path

    url = task['url']
    file_par_path, file_path, log_path = make_dirs()
    time1 = time.time()
    status, output = wget_download()
    time2 = time.time()
    file_size = get_file_size()
    if 0 == status and file_size > 0:
        file_md5 = get_file_md5()
        down_speed = get_down_speed()
        # file_path = rename_file_name()
        logger.info('download ok, len={}Mb, md5={}, speed={}kb/s, {}'.format(
            format(file_size / 1024.0**2, '0.2f'),
            file_md5, down_speed, file_path))
        return True, (file_md5, file_path, file_size, down_speed)
    else:
        logger.warn('download fail, output={}, url={}, log={}'.format(
            output, url, log_path))
        return False, file_size


def valid_message(task):
    return 'id' in task and 'url' in task and \
        'domain' in task and 'filename' in task and 'date' in task and \
        'csv_file' in task and 'csv_file_number' in task


def get_same_file_info(db_pool, url_date, filename, domain, max_days):
    def load_same_tasks(url_date, filename, domain, max_days):
        sql = FETCH_SAME_TASK.format(
            date=url_date, name=filename, domain=domain, max_days=max_days)
        # logger.debug('check same file:{}'.format(sql))
        rc, rs = yield db_query(sql)
        yield db_result(rc, rs)
    if len(filename) > config_file_name_len:
        _, res = db_txn(db_pool, partial(
            load_same_tasks, url_date, filename, domain, max_days))
        if res:
            return (res[0].id, res[0].file_md5, res[0].video_path)


def update_same_file_info(db_pool, task):
    def _up_status(task):
        sql = UPDATE_SAME_TASK.format(
            md5=task['file_md5'], bid=task['brother_id'], id=task['id'],
            vpath=task['video_path'])
        # logger.debug('update_same_file_info: {}'.format(sql))
        yield db_execute(sql)
    db_txn(db_pool, partial(_up_status, task))


def update_download_ok(db_pool, task):
    def _up_status(task):
        sql = DOWNLOAD_SUCCESS.format(
            md5=task['file_md5'], id=task['id'], vpath=task['video_path'],
            len=task['file_size'], speed=task['down_speed'])
        # logger.debug('update_download_ok: {}'.format(sql))
        yield db_execute(sql)
    db_txn(db_pool, partial(_up_status, task))


def update_download_fail(db_pool, id, file_size):
    def _up_status(id, file_size):
        sql = DOWNLOAD_FAIL.format(id=id, file_size=file_size)
        # logger.debug('update_download_fail: {}'.format(sql))
        yield db_execute(sql)
    db_txn(db_pool, partial(_up_status, id, file_size))


def send(querier_mq, task):
    push_info = {}
    push_info['id'] = task['id']
    push_info['file_md5'] = task['file_md5']
    push_info['csv_file'] = task['csv_file']
    push_info['csv_file_number'] = task['csv_file_number']
    push_info['video_path'] = task['video_path']
    push_info['url_date'] = task['date']
    querier_mq.send(push_info, retry=True)
    logger.debug('send to querier_queue,info={}'.format(push_info))


def run(querier_queue_url, download_queue_url, download_re_queue_url, db_url):
    def process_task(task, message):
        task = trans2utf8(task)
        logger.message_decorate(url_id=task['id'])
        if valid_message(task):
            logger.info('receive rabbitmq: {}'.format(task))
            file_size = 0
            try:
                same_ret = get_same_file_info(
                    db_pool, task['date'], task['filename'], task['domain'],
                    same_file_name_max_days)
                if same_ret:
                    logger.info('found same file name:{} {}'.format(
                        task['filename'], task['domain']))
                    task['brother_id'] = same_ret[0]
                    task['file_md5'] = same_ret[1]
                    task['video_path'] = same_ret[2]
                    update_same_file_info(db_pool, task)
                    send(querier_mq, task)
                else:
                    logger.info('start download, id={}, url={}'.format(
                        task['id'], task['url']))
                    status, download_ret = download(task)
                    if not status and \
                            task['download_count'] >= config_download_count:
                        for v in config_video_type:
                            pattern = re.compile(config_re_find.format(v))
                            m = pattern.findall(task['url'])
                            if m:
                                logger.info('download new url={}'.format(m[0]))
                                task['url'] = m[0]
                                status, download_ret = download(task)
                                break
                    if status:
                        task['file_md5'] = download_ret[0]
                        task['video_path'] = download_ret[1]
                        task['file_size'] = download_ret[2]
                        task['down_speed'] = download_ret[3]
                        update_download_ok(db_pool, task)
                        send(querier_mq, task)
                    else:
                        file_size = download_ret
                        update_download_fail(db_pool, task['id'], download_ret)
            except OperationalError:
                logger.error('mysql operator error id={}, error={}'.format(
                    task['id'], traceback.format_exc()))
                message.requeue()
            except:
                update_download_fail(db_pool, task['id'], file_size)
                logger.error('deal with download error id={},{}'.format(
                    task['id'], traceback.format_exc()))
                message.ack()
            else:
                message.ack()
        else:
            logger.warn('receive invalid queue info: {}'.format(task))
            message.ack()
        logger.message_undecorate()
    querier_mq = Amqp(querier_queue_url, querier_mq_exchange,
                      querier_mq_queue, querier_mq_routing_key)
    db_pool = make_pool(db_url)
    if g_bDownloader_re:
        with Amqp(download_re_queue_url, re_mq_exchange,
                  re_mq_queue, re_mq_routing_key) as q:
            q.poll(process_task)
    else:
        with Amqp(download_queue_url, mq_exchange,
                  mq_queue, mq_routing_key) as q:
            q.poll(process_task)


def main():
    try:
        logger.info('{} start'.format(
            'downloader_re' if g_bDownloader_re else 'downloader'))
        logger.setLevel(log_level)
        with open(join(PAR_DIR, 'etc', 'config.json'), 'r') as f:
            config = json.loads(f.read())
        run(config['query_queue'], config['download_queue'],
            config['download_retry_queue'], config['work_db'])
    except:
        logger.error("downloader_unhandle_except: {}".format(
            traceback.format_exc()))


if __name__ == '__main__':
    main()
