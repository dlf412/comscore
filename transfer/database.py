#!/usr/bin/env python
# encoding: utf-8
__author__ = 'deng_lingfei'

from functools import partial
import time
import os
import myutils
from db_txn import db_txn, db_insert, db_query, db_result
from utils import make_pool


INFO_DAILYS = '''select distinct daily from dailyInput order by daily desc'''
QUERY_DAILYINFOS = '''select daily, dl_start, dl_end, im_start, im_end, files, im_files, status, local_dir
from dailyInput where daily >= {ago} order by daily'''
SAVE_DAILYINFO = '''insert into dailyInput (daily, files, im_files,
status, dl_start, dl_end, im_start, im_end, local_dir)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
on duplicate key update
    files = values(files), im_files = values(im_files),
    status = values(status), dl_start = values(dl_start),
    dl_end = values(dl_end), im_start = values(im_start),
    im_end = values(im_end), local_dir = values(local_dir)'''

RESULT_DAILYS = '''select distinct daily from dailyReturn order by daily desc'''
QUERY_DAILYRESULTS = '''select daily, ex_start, ex_end, ul_start, ul_end, files, ul_files, status, local_dir
from dailyReturn where daily >= {ago} order by daily'''
SAVE_DAILYRESULT = '''insert into dailyReturn (daily, files, ul_files,
status, ex_start, ex_end, ul_start, ul_end, local_dir)
values (%s, %s, %s, %s, %s, %s, %s, %s, %s)
on duplicate key update
    files = values(files), ul_files = values(ul_files),
    status = values(status), ex_start = values(ex_start),
    ex_end = values(ex_end), ul_start = values(ul_start),
    ul_end = values(ul_end), local_dir = values(local_dir)'''


class DailyInfo(object):

    def __init__(self, daily, local_dir):
        self.dl_end = self.dl_start = int(time.time())
        self.im_end = self.im_start = 0
        self.files = []
        self.daily = daily
        self.im_files = []
        self.status = 'new'
        self.local_dir = local_dir

    def __str__(self):
        return "daily:{}, status:{}, files:{}, import files:{}, " \
               "download cost:{}, import cost:{}".format(self.daily,
                                                         self.status,
                                                         self.files,
                                                         self.im_files,
                                                         self.dl_end - self.dl_start,
                                                         self.im_end - self.im_start)


class DailyResult(object):

    def __init__(self, daily, local_dir):
        self.ex_end = self.ex_start = int(time.time())
        self.ul_end = self.ul_start = 0
        self.files = []
        self.daily = daily
        self.ul_files = []
        self.status = 'new'
        self.local_dir = local_dir

    def __str__(self):
        return "daily:{}, status:{}, files:{}, upload files:{}, " \
               "upload cost:{}, export cost:{}".format(self.daily,
                                                       self.status,
                                                       self.files,
                                                       self.ul_files,
                                                       self.ul_end - self.ul_start,
                                                       self.ex_end - self.ex_start)


def _querydb(sql, *args):
    rc, rs = yield db_query(sql, *args)
    yield db_result(rc, rs)


def _savedb(sql, *args):
    yield db_insert(sql, *args)


def getDailyInfos(dbpool, ago=1):
    day_ago = myutils.date_offset_today(0 - ago)
    rc, dids = db_txn(dbpool, partial(
        _querydb, QUERY_DAILYINFOS.format(ago=day_ago)))
    dailys = []

    if rc == 0:
        return dailys

    for did in dids:
        daily = DailyInfo(did.daily, did.local_dir)
        im_files = filter(lambda a: a.strip() != '', did.im_files.split(','))
        files = filter(lambda a: a.strip() != '', did.files.split(','))

        daily.files = [os.path.join(did.local_dir,
                                    'DailyInput',
                                    did.daily[:4] + '_' + did.daily[4:6],
                                    did.daily, f) for f in files]
        daily.im_files = [os.path.join(did.local_dir,
                                       'DailyInput',
                                       did.daily[:4] + '_' + did.daily[4:6],
                                       did.daily, f) for f in im_files]
        daily.dl_end = did.dl_end
        daily.dl_start = did.dl_start
        daily.status = did.status
        daily.im_start = did.im_start
        daily.im_end = did.im_end
        dailys.append(daily)
    return dailys


def saveDailyInfo(dbpool, info):
    files = ','.join([os.path.basename(f) for f in info.files])
    im_files = ','.join([os.path.basename(f) for f in info.im_files])
    db_txn(dbpool, partial(_savedb, SAVE_DAILYINFO),
           info.daily, files, im_files, info.status,
           info.dl_start, info.dl_end, info.im_start,
           info.im_end, info.local_dir)


def getDailyResults(dbpool, results_ago, ago=8):
    day_ago = myutils.date_offset_today(0 - results_ago - ago)
    rc, dids = db_txn(dbpool, partial(
        _querydb, QUERY_DAILYRESULTS.format(ago=day_ago)))
    dailys = []

    if rc == 0:
        return dailys

    for did in dids:
        daily = DailyResult(did.daily, did.local_dir)
        ul_files = filter(lambda a: a.strip() != '', did.ul_files.split(','))
        files = filter(lambda a: a.strip() != '', did.files.split(','))

        daily.files = [os.path.join(did.local_dir,
                                    'DailyReturn',
                                    did.daily[:4] + '_' + did.daily[4:6],
                                    did.daily, f) for f in files]
        daily.ul_files = [os.path.join(did.local_dir,
                                       'DailyReturn',
                                       did.daily[:4] + '_' + did.daily[4:6],
                                       did.daily, f) for f in ul_files]
        daily.ex_end = did.ex_end
        daily.ex_start = did.ex_start
        daily.status = did.status
        daily.ul_start = did.ul_start
        daily.ul_end = did.ul_end
        dailys.append(daily)
    return dailys


def saveDailyResult(dbpool, result):
    files = ','.join([os.path.basename(f) for f in result.files])
    ul_files = ','.join([os.path.basename(f) for f in result.ul_files])
    db_txn(dbpool, partial(_savedb, SAVE_DAILYRESULT),
           result.daily, files, ul_files, result.status,
           result.ex_start, result.ex_end, result.ul_start,
           result.ul_end, result.local_dir)


def dailyinfoNews(dbpool, ago):
    '''
    :param dbpool:
    :param ago:
    :return: history dates list
    '''
    rc, dailys = db_txn(dbpool, partial(_querydb, INFO_DAILYS))
    if rc:
        dids = map(lambda u: u.daily, dailys[0:ago])
    else:
        dids = []
    shoulds = [myutils.date_offset_today(d) for d in range(0 - ago, 0)]
    return sorted(list(set(shoulds) - set(dids)))


def dailyresultNews(dbpool, results_ago, ago=7):
    '''
    :param dbpool:
    :param ago:
    :return: history dates list
    '''
    rc, dailys = db_txn(dbpool, partial(_querydb, RESULT_DAILYS))
    if rc:
        dids = map(lambda u: u.daily, dailys[0:ago])
    else:
        dids = []
    shoulds = [myutils.date_offset_today(d) for d in range(
        0 - ago - results_ago, 0 - results_ago)]
    return sorted(list(set(shoulds) - set(dids)))


if __name__ == '__main__':
    db = "mysql://mediawise:123@192.168.1.34:3306/comScoreTmp"
    dbpool = make_pool(db)
    dailyinfo = DailyInfo('20160825', './')
    dailyinfo.files = ['./DailyInput/2016_08/20160825/url_20160825-1.csv.gz']
    dailyinfo.dl_start = int(time.time())
    dailyinfo.dl_end = int(time.time()) + 60
    dailyinfo.im_start = int(time.time())
    dailyinfo.im_end = int(time.time()) + 60
    dailyinfo.status = 'import_ok'
    str_daily = str(dailyinfo)
    print str_daily
    saveDailyInfo(dbpool, dailyinfo)
    ago = int(myutils.date_offset_today()) - 20160825
    str_daily_ex = str(getDailyInfos(dbpool, ago)[0])
    print str_daily_ex
    assert str_daily == str_daily_ex

    dailyresult = DailyResult('20160701', './')
    dailyresult.files = [
        './DailyReturn/2016_07/20160701/url_20160701-1_result.csv.gz']
    dailyresult.ex_start = int(time.time())
    dailyresult.ex_end = int(time.time()) + 60
    dailyresult.ul_start = int(time.time())
    dailyresult.ul_end = int(time.time()) + 60
    dailyresult.status = 'upload_ok'
    str_daily = str(dailyresult)
    print str_daily
    saveDailyResult(dbpool, dailyresult)

    ago = int(myutils.date_offset_today()) - 20160701 + 8
    print ago
    str_daily_ex = str(getDailyResults(dbpool, ago)[0])
    print str_daily_ex
    assert str_daily == str_daily_ex
