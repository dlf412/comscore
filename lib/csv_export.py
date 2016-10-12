#!/usr/bin/env python
# encoding: utf-8
import csv
import os
from db_txn import db_txn, db_query, db_result, db_execute
from functools import partial
from itertools import groupby
import gzip
from utils import make_pool


EMPTY_PREFIX = '#@*$#VOBILE#@*$#'

ONEDAY = """select ad_idc, url, file_name as filename,
domain as publisher_name, csv_file, csv_file_number,
date_format(url_date, '%%Y%%m%%d') as url_date,
IFNULL(match_meta, 'Fail') as metaID
from url where url_date = {}"""

MANYDAYS = """select ad_idc, url, file_name as filename,
domain as publisher_name, csv_file, csv_file_number,
date_format(url_date, '%%Y%%m%%d') as url_date,
IFNULL(match_meta, 'Fail') as metaID
from url where url_date in ({})"""

ALLDAYS = """select ad_idc, url, file_name as filename,
domain as publisher_name, csv_file, csv_file_number,
date_format(url_date, '%%Y%%m%%d') as url_date,
IFNULL(match_meta, 'Fail') as metaID
from url"""

VIDEOONEDAY = """select filename, IFNULL(match_meta, 'Fail') as metaID,
video_date, number
from referenceMeta
where video_date={}"""

VIDEOMANYDAYS = """select filename, IFNULL(match_meta, 'Fail') as metaID,
video_date, number
from referenceMeta
where video_date in ({})"""

VIDEOALLDAYS = """select filename, IFNULL(match_meta, 'Fail') as metaID,
video_date, number
from referenceMeta"""


class CSVExportor(object):

    _groupby = None
    _sortby = None
    _headers = []

    def __init__(self, dbpool, sql, *args):
        '''
        init db, sql and sql_args
        '''
        self._dbpool = dbpool
        self._sql = sql
        self._args = args

    @staticmethod
    def _querydb(sql, *args):
        _, rs = yield db_query(sql, *args)
        yield db_result(rs)

    @staticmethod
    def _updatedb(sql, *args):
        yield db_execute(sql, *args)

    def _queried(self):
        queries = db_txn(self._dbpool,
                         partial(self._querydb, self._sql), *self._args)
        return [] if queries is None else queries

    def _grouped(self, queried):
        '''
        arguments: [namedtuple....]
        return '{self._groupby:[namedtuple ....]}'
        '''
        if self._groupby:
            sort_key = lambda q: getattr(q, self._groupby)
            queried.sort(key=sort_key)
            grouper = groupby(queried, key=sort_key)
            return {key: list(g) for key, g in grouper}
        else:
            return {self.__class__.__name__, queried}

    def _sorted(self, grouped):
        if self._sortby:
            for value in grouped.values():
                value.sort(key=lambda q: getattr(q, self._sortby))
            return grouped
        else:
            return grouped

    def _results(self):
        return self._sorted(self._grouped(self._queried()))

    @staticmethod
    def _elem2dict(results):
        to_dict = lambda r: r._asdict()
        return map(to_dict, results)

    def _reserve_headers(self, results):
        if not self._headers:
            return

        if results:
            remove_keys = set(results[0].keys()) - set(self._headers)

        for r in results:
            for key in remove_keys:
                del r[key]

    def _before_dump(self, results):
        '''
        can be overwritted by subclass
        always do nothing: pass
        '''
        pass

    def _after_dump(self):
        '''
        can be overwritted by subclass
        always do nothing: pass
        '''
        pass

    def gen_dump_file(self, to_dir, rc, key):
        '''
        key: url_20160817-1_result.csv.gz
        #/DailyReturn/2016_08/20160817/url_20160817-1_result.csv.gz
        The method should be overwritted by subclass
        '''

        return os.path.join(to_dir, key)

    def dump2csv(self, to_dir, compress=True):
        '''
        csv file format: self._headers
        '''
        dump_files = []
        _open = gzip.open if compress else open

        results = self._results()

        if isinstance(results, list):
            results = {self.__class__.__name__: results}

        for key, results in self._results().items():
            dump_file = self.gen_dump_file(to_dir, results[0], key)
            # transtolate dict to modify
            rc = self._elem2dict(results)
            self._reserve_headers(rc)
            self._before_dump(rc)
            with _open(dump_file, 'wb') as fp:
                dict_writer = csv.DictWriter(fp, fieldnames=rc[0].keys())
                dict_writer.writeheader()
                dict_writer.writerows(rc)
            dump_files.append(dump_file)
            self._after_dump()
        return dump_files


class Results(CSVExportor):
    '''
    export url table to csv files by url_date
    '''
    _groupby = 'csv_file'
    _sortby = 'csv_file_number'
    _headers = ['ad_idc', 'url', 'filename',
                'publisher_name', 'metaID']

    def __init__(self, dbpool, *url_dates):

        url_dates = ["'%s'" % day for day in list(url_dates)]
        sql = ALLDAYS
        if len(url_dates) == 1:
            sql = ONEDAY.format(url_dates[0])
        elif len(url_dates) > 1:
            sql = MANYDAYS.format(','.join(url_dates))
        super(self.__class__, self).__init__(dbpool, sql)

    def _before_dump(self, results):
        '''
        should be overwritted by subclass
        always do nothing: pass
        '''
        self._fix_filename_and_domain(results)

    @staticmethod
    def _fix_filename_and_domain(results):
        for r in results:
            if r['filename'] == EMPTY_PREFIX + r['ad_idc']:
                r['filename'] = ''
            if r['publisher_name'] == EMPTY_PREFIX + r['ad_idc']:
                r['publisher_name'] = ''

    def gen_dump_file(self, to_dir, rc, key):
        '''
        key: url_20160817-1_result.csv.gz
        #/DailyReturn/2016_08/20160817/url_20160817-1_result.csv.gz

        The method should be overwritted by subclass
        '''
        dump_dir = os.path.join(to_dir, 'DailyReturn',
                                rc.url_date[:4] + '_' + rc.url_date[4:6],
                                rc.url_date)
        if not os.path.exists(dump_dir):
            os.makedirs(dump_dir)

        f_s = key.split('.', 1)

        if len(f_s) == 1:
            dump_file = os.path.join(dump_dir, f_s[0] + '_result')
        else:
            dump_file = os.path.join(dump_dir, f_s[0] + '_result.' + f_s[1])

        return dump_file


class VideoResults(CSVExportor):
    '''
    export referenceMeta table to csv files by url_date
    '''
    _groupby = 'video_date'
    _sortby = 'number'
    _headers = ['filename', 'metaID']

    def __init__(self, dbpool, *video_dates):

        video_dates = ["'%s'" % day for day in list(video_dates)]
        sql = VIDEOALLDAYS
        if len(video_dates) == 1:
            sql = VIDEOONEDAY.format(video_dates[0])
        elif len(video_dates) > 1:
            sql = VIDEOMANYDAYS.format(','.join(video_dates))
        super(self.__class__, self).__init__(dbpool, sql)

    def gen_dump_file(self, to_dir, rc, key):
        '''
        csv_file: $to_dir/AdsIngestion/2016_07/20160712/ads_20160712-1.csv.gz
        key: 20160817
        The method should be overwritted by subclass
        '''
        dump_dir = os.path.join(to_dir, 'AdsIngestion',
                                key[:4] + '_' + key[4:6],
                                key)
        if not os.path.exists(dump_dir):
            os.makedirs(dump_dir)

        csv_file = 'ads_' + key + '-1.csv.gz'
        dump_file = os.path.join(dump_dir, csv_file)

        return dump_file


if __name__ == '__main__':
    db = "mysql://mediawise:123@192.168.1.34:3306/comScoreXX"
    dbpool = make_pool(db)

    results = Results(dbpool, '20160829', '20160828')
    print results.dump2csv('./')

    results = VideoResults(dbpool, '20160901', '20160902')
    print results.dump2csv('./')
