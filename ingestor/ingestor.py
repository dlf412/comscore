#!/usr/bin/env python
# encoding: utf-8
from __future__ import division
from os.path import join
import json
import time
import datetime

from config import (config_down_ftp, config_upload_ftp,
                    config_vobile_email, config_comscore_email,
                    config_match_days, config_day_time,
                    config_check_day_interval)
from include import PAR_DIR, logger, video_search
from db_access import AccessDB
from ftp_sync import FTPSync
from reference_logic import process
from ingestor_email import EmailDeliver
from utils import Retry
from query_utils import (query as query_dna, ingest as ingest_dna,
                         QueryDnaError, IngestDnaError, RedisConnectError)


@Retry(10512000, exceptions=(RedisConnectError,),
       delay=3, logger=logger)
def check_no_match_dna(db_conn, local_file, key, frame, file_md5, config):
    ret = query_dna(video_search, local_file, key, frame,
                    config['redis']['host'],
                    config['redis']['port'],
                    config['redis']['password'])
    if ret:
        logger.info('no match url query found dna, meta={}'.format(ret))
        db_conn.update_no_match_url(file_md5, ret)
    else:
        meta_key = ingest_dna(video_search, local_file, key, frame,
                              config['redis']['host'],
                              config['redis']['port'],
                              config['redis']['password'])
        logger.info('no match url ingest dna ok, file={}, meta={}'
                    .format(local_file, meta_key))
        db_conn.update_no_match_url(file_md5, meta_key)


def ingestor_no_match(day, db_conn, config):
    logger.info('start fetch no match videos to ingest')
    res = db_conn.fetch_no_match_url(day)
    while res:
        id, file_md5, local_file, feature_dir, url_date = res[0]
        logger.message_decorate(url_id=id)
        logger.info('fetch no match url, url_date={}'.format(url_date))
        ref = "-".join([file_md5[:8], file_md5[8:12], file_md5[12:16],
                        file_md5[16:20], file_md5[20:]])
        key = join(feature_dir, ref)
        frame = join(feature_dir, "frame")
        try:
            check_no_match_dna(
                db_conn, local_file, key, frame, file_md5, config)
        except QueryDnaError, e:
            logger.error('QueryDnaError {}'.format(e))
        except IngestDnaError, e:
            logger.error('IngestDnaError {}'.format(e))
        logger.message_undecorate()
        res = db_conn.fetch_no_match_url(day)


def check_time_run(first_run, d_now, check_day, hour, minute):
    if first_run:
        return True
    if d_now.hour >= hour and d_now.minute >= minute:
        if d_now.strftime('%Y%m%d') != check_day.strftime('%Y%m%d'):
            return True
        else:
            logger.info('day {} has been check'.format(d_now))


def do_days(db_conn, d_now, down_ftp, upload_ftp, config, email, manual):
    for i_pre in range(config_match_days, 1)[::-1]:
        d_pre = d_now + datetime.timedelta(i_pre)
        day = d_pre.strftime('%Y%m%d')
        if not db_conn.is_referenceDay_exists(day):
            process(day, down_ftp, upload_ftp, db_conn, config, email, manual)


def run(down_ftp, upload_ftp, db_conn, config, email):
    check_day = None
    first_run = True
    hour, minute = config_day_time.split(':')
    hour, minute = int(hour), int(minute)
    while True:
        d_now = datetime.datetime.utcnow()
        if not check_day:
            check_day = d_now + datetime.timedelta(-1)
        if check_time_run(first_run, d_now, check_day, hour, minute):
            first_run = False
            check_day = d_now
            logger.info('start download ftp from {}'.format(d_now))
            do_days(db_conn, d_now, down_ftp, upload_ftp, config, email, False)
            ingestor_no_match(d_now.strftime('%Y%m%d'), db_conn, config)
        else:
            logger.info('today ftp scan end, sleep {} seconds'
                        .format(config_check_day_interval))
            time.sleep(config_check_day_interval)


def main():
    logger.info('ingestor start')
    config = json.load(open(join(PAR_DIR, 'etc', 'config.json'), 'r'))
    assert('redis' in config)
    db_conn = AccessDB(config['work_db'])
    down_ftp = FTPSync(config_down_ftp['host'], config_down_ftp['user'],
                       config_down_ftp['passwd'])
    upload_ftp = FTPSync(config_upload_ftp['host'], config_upload_ftp['user'],
                         config_upload_ftp['passwd'])
    email = EmailDeliver(config_comscore_email, config_vobile_email)
    run(down_ftp, upload_ftp, db_conn, config, email)


if __name__ == '__main__':
    try:
        main()
    except:
        logger.error('ingestor_unhandle_except', exc_info=True)
