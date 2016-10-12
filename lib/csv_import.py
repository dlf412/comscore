#!/usr/bin/env python
# encoding: utf-8
import sys
import csv
import time
#from functools import partial
import os
from db_txn import db_txn, db_insert
import gzip
import warnings
from utils import make_pool


EMPTY_PREFIX = '#@*$#VOBILE#@*$#'

SAVE_SQL = """INSERT INTO url SET ad_idc=%s, file_name=%s,
url=%s, url_date=%s, domain=%s,
csv_file=%s, csv_file_number=%s"""


class URLReader(object):

    def __init__(self, csv_file, url_date=None, compress=True):
        '''
        csv file like 'url_yyyymmdd-no.csv'
        '''
        self._csv_file_abspath = csv_file
        self._csv_file = os.path.basename(csv_file)
        if url_date:
            self._url_date = url_date
        else:
            self._url_date = self._csv_file.split('-')[0][-8:]
        # check url_date is valid or not
        time.strptime(self._url_date, '%Y%m%d')
        self._open = gzip.open if compress else open

    def __iter__(self):
        with self._open(self._csv_file_abspath, 'rb') as fp:
            for url in csv.DictReader(fp):
                yield url

    def all(self):
        return list(self)

    def process(self, callback):
        '''
        the callback is a function with url_info argument
        '''

        for url_info in self:
            callback(url_info)

    def show_header(self, n=10):
        m = 0
        for row in self:
            m += 1
            if m <= n:
                print row
            else:
                break

    def save2db(self, dbpool):
        '''
        using SAVE_SQL save csv file's rows
        csv file will not be saved if any row save failed
        '''
        para_dict = {"csv_file": self._csv_file,
                     "url_date": self._url_date, "row_no": 0}

        def _save():
            for url in self:
                para_dict['row_no'] += 1
                para_dict.update(url)
                if not para_dict['filename']:
                    para_dict['filename'] = EMPTY_PREFIX + para_dict['ad_idc']
                if not para_dict['publisher_name']:
                    para_dict['publisher_name'] = EMPTY_PREFIX + \
                        para_dict['ad_idc']
                if len(para_dict['filename']) > 256 \
                        or len(para_dict['publisher_name']) > 256 \
                        or len(para_dict['url']) > 2048:
                    warnings.warn("row:[%d] filename, "
                                  "pubilsher_name or url too long".format(para_dict['row_no']))
                # print para_dict['row_no']
                #sql = SAVE_SQL.format(**para_dict)
                # print sql
                yield db_insert(
                    SAVE_SQL,
                    para_dict['ad_idc'],
                    para_dict['filename'],
                    para_dict['url'],
                    para_dict['url_date'],
                    para_dict['publisher_name'],
                    para_dict['csv_file'],
                    para_dict['row_no']
                )
        db_txn(dbpool, _save)


if __name__ == '__main__':
    csvf = sys.argv[1]
    urlreader = URLReader(csvf, compress=True)

    db = "mysql://mediawise:123@192.168.1.34:3306/comScoreTmp"

    dbpool = make_pool(db)

    urlreader.save2db(dbpool)

    def callback(url_info):
        '''
        url_info is a dict as follows
        {
        'url': '07251019.float.kankan.com/finalfiles/n1464601982221.flv',
        'publisher_name': 'kankan.com',
        'ad_idc': 'wXSCYpVnQhVq1gSOtwWj9b',
        'filename': 'n1464601982221.flv'
        }
        '''

        if len(url_info['publisher_name']) == 0:
            print url_info

        elif len(url_info['filename']) == 0:
            print url_info

    # urlreader.process(callback)
