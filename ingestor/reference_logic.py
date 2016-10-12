#!/usr/bin/env python
# encoding: utf-8
from __future__ import division
import os
from os.path import join, basename
import datetime

from config import (config_fail_max_count, config_fail_max_percent,
                    config_check_ftp_frequency, config_check_ftp_interval)
from include import PAR_DIR, logger, video_search
from ftp_sync import (reference_download, FileNotExists,
                      ingest_upload)
from query_utils import (create_dna, query as query_dna, ingest as ingest_dna,
                         CreateDnaError, QueryDnaError, IngestDnaError, RedisConnectError)
from csv_export import VideoResults
from utils import Retry


def createPath(date, add_dir):
    '''date must like 20060909'''
    path = join(PAR_DIR, 'var', 'reference',
                '{}_{}'.format(date[0: 4], date[4: 6]),
                date, add_dir)
    if not os.path.exists(path):
        os.makedirs(path)
    return path


def get_ftp_files(down_ftp, day, email, manual):
    ret_dict = {}
    ret_dict['localdir'] = createPath(day, 'meta')
    all_files, md5_error_file, invalid_file = reference_download(
        down_ftp, day, ret_dict['localdir'])
    ret_dict['all_files'] = all_files
    if md5_error_file:
        logger.error('{} files md5 check error on {}'.format(
            len(md5_error_file), day))
        # if not manual:
        #     email.send_mycompany_md5_fail(day, md5_error_file, invalid_file)
    elif invalid_file:
        logger.error('{} filelist.txt {} files not exists'.format(
            day, len(invalid_file)))
        # if not manual:
        #     email.send_invalid_file(day, invalid_file)
    ret_dict['md5_error_files'] = md5_error_file
    ret_dict['error_files'] = md5_error_file + invalid_file
    return ret_dict


def file_size(local_file):
    if os.access(local_file, os.R_OK):
        return os.stat(local_file).st_size
    return 0


@Retry(10512000, exceptions=(RedisConnectError,), delay=3,
       logger=logger)
def video2dna(day, db_conn, full_path, feature_dir, number, config):
    b_ret = True
    try:
        logger.info('create dna {}'.format(full_path))
        file_name = basename(full_path)
        file_md5 = os.path.splitext(file_name)[0]
        key, frame = create_dna(video_search, full_path, file_md5,
                                feature_dir)
        ret = query_dna(video_search, full_path, key, frame,
                        config['redis']['host'], config['redis']['port'],
                        config['redis']['password'])
        if ret:
            logger.info('query found dna, meta={}'.format(ret))
            db_conn.insert_referenceMeta('dna_ok', day,
                                         full_path, number,
                                         file_size(full_path),
                                         ret, file_name)
        else:
            meta_key = ingest_dna(video_search, full_path, key, frame,
                                  config['redis']['host'],
                                  config['redis']['port'],
                                  config['redis']['password'])
            logger.info('ingest dna ok, file={}, meta={}'.format(
                full_path, meta_key))
            db_conn.insert_referenceMeta('insert_ok', day, full_path, number,
                                         file_size(full_path), meta_key,
                                         file_name)
    except CreateDnaError, e:
        b_ret = False
        logger.error('CreateDnaError {} {}'.format(e, full_path))
        db_conn.insert_referenceMeta('dna_err', day, full_path, number,
                                     file_size(full_path), None, file_name)
    except QueryDnaError, e:
        b_ret = False
        logger.error('QueryDnaError {} {}'.format(e, full_path))
        db_conn.insert_referenceMeta('dna_err', day, full_path, number,
                                     file_size(full_path), None, file_name)
    except IngestDnaError, e:
        b_ret = False
        logger.error('IngestDnaError {} {}'.format(e, full_path))
        db_conn.insert_referenceMeta('dna_err', day, full_path, number,
                                     file_size(full_path), None, file_name)
    return b_ret


def create_dna_2_insert(db_conn, day, all_files_dict, config, email, manual):
    logger.info('videos start to create dna on {}'.format(day))
    feature_dir = createPath(day, 'feature')
    all_files = all_files_dict['all_files']
    localdir = all_files_dict['localdir']
    number = 1
    error_files = all_files_dict['error_files']
    md5_error_files = all_files_dict['md5_error_files']
    create_dna_error_files = []
    for fileName in all_files:
        full_path = join(localdir, fileName)
        if fileName in error_files:
            status = 'no_exists'
            if fileName in md5_error_files:
                status = 'md5_err'
            db_conn.insert_referenceMeta(status, day, full_path, number,
                                         file_size(full_path), None, fileName)
            number += 1
            continue
        ret = video2dna(day, db_conn, full_path, feature_dir, number, config)
        if not ret:
            create_dna_error_files.append(fileName)
        number += 1
    all_files_dict['create_dna_fail_files'] = create_dna_error_files


def export_csv(db_pool, day):
    logger.info('export csv file on {}'.format(day))
    results = VideoResults(db_pool, day)
    csv_path = createPath(day, 'csv')
    dump_files = results.dump2csv(csv_path)
    assert(len(dump_files) == 1)
    return csv_path


def check_liminal(day, d_all):
    all_files_num = len(d_all['all_files'])
    error_files_num = len(d_all['error_files']) + \
        len(d_all['create_dna_fail_files'])
    if all_files_num > config_fail_max_count and \
            (error_files_num / all_files_num) * 100 > config_fail_max_percent:
        logger.error('{} {}/{} video files fail more than {} and {}%'.
                     format(day, error_files_num, all_files_num,
                            config_fail_max_count,
                            config_fail_max_percent))
        return False
    else:
        return True


@Retry(config_check_ftp_frequency, exceptions=(FileNotExists,),
       delay=config_check_ftp_interval, logger=logger)
def run(day, down_ftp, upload_ftp, db_conn, config, email, manual):
    logger.info('downloading from ftp on {}'.format(day))
    start = datetime.datetime.utcnow()
    d_ret = get_ftp_files(down_ftp, day, db_conn, config)
    create_dna_2_insert(db_conn, day, d_ret, config, email, manual)
    b_liminal_ok = check_liminal(day, d_ret)
    all_files_num = len(d_ret['all_files'])
    err_all = d_ret['error_files'] + d_ret['create_dna_fail_files']
    if manual or b_liminal_ok:
        csv_path = export_csv(db_conn.pool(), day)
        ingest_upload(upload_ftp, day, csv_path)
        email.send_success(day, err_all)
        db_conn.insert_referenceDay(day, all_files_num, 'true', start)
    else:
        email.send_fail(day, err_all)
        db_conn.insert_referenceDay(day, all_files_num, 'false', start)


def process(day, down_ftp, upload_ftp, db_conn, config, email, manual):
    ret = True
    try:
        run(day, down_ftp, upload_ftp, db_conn, config, email, manual)
    except FileNotExists, e:
        logger.error(str(e))
        email.send_filelist_miss(day)
        ret = False
    return ret
