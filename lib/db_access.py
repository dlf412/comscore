#!/usr/bin/env python
# encoding: utf-8
from functools import partial
from MySQLdb import OperationalError
from include import logger
from db_txn import db_execute, db_query, db_result, db_txn, db_insert
from utils import make_pool, Retry


class AccessDB(object):
    def __init__(self, url):
        self.db_pool = make_pool(url)

    def pool(self):
        return self.db_pool

    @Retry(10512000, exceptions=(OperationalError,), delay=3, logger=logger)
    def is_referenceDay_exists(self, referenceDay):
        def _exists(referenceDay):
            sql = '''SELECT 1 AS e FROM referenceDay
                    WHERE video_date='{}' '''.format(referenceDay)
            rc, rs = yield db_query(sql)
            yield db_result(rc, rs)
        _, res = db_txn(self.db_pool, partial(_exists, referenceDay))
        if res:
            return True

    @Retry(10512000, exceptions=(OperationalError,), delay=3, logger=logger)
    def is_referenceDay_lock(self, referenceDay):
        def _exists(referenceDay):
            sql = '''SELECT 1 AS e FROM referenceDay
                    WHERE video_date='{}'
                    AND status='false' '''.format(referenceDay)
            rc, rs = yield db_query(sql)
            yield db_result(rc, rs)
        _, res = db_txn(self.db_pool, partial(_exists, referenceDay))
        if res:
            return True

    @Retry(10512000, exceptions=(OperationalError,), delay=3, logger=logger)
    def insert_referenceDay(self, referenceDay, num, lock, start):
        def _insert(referenceDay, num, lock, start):
            sql = '''INSERT INTO referenceDay
                    SET video_date=%s,num=%s,status=%s,start=%s,end=now()'''
            yield db_insert(sql, referenceDay, num, lock, start)

        def _update(referenceDay, num, lock, start):
            sql = '''UPDATE referenceDay SET num=%s,status=%s,start=%s,end=now()
                        WHERE video_date=%s'''
            yield db_execute(sql, num, lock, start, referenceDay)
        if referenceDay and self.is_referenceDay_exists(referenceDay):
            db_txn(self.db_pool, partial(
                _update, referenceDay, num, lock, start))
        else:
            db_txn(self.db_pool, partial(
                _insert, referenceDay, num, lock, start))

    @Retry(10512000, exceptions=(OperationalError,), delay=3, logger=logger)
    def is_referenceMeta_video_exists(self, referenceDay, number):
        def _exists(referenceDay, number):
            sql = '''SELECT 1 AS e FROM referenceMeta
                    WHERE video_date=%s AND number=%s'''
            rc, rs = yield db_query(sql, referenceDay, number)
            yield db_result(rc, rs)
        _, res = db_txn(self.db_pool, partial(_exists, referenceDay, number))
        if res:
            return True

    @Retry(10512000, exceptions=(OperationalError,), delay=3, logger=logger)
    def insert_referenceMeta(self, status, referenceDay, video_path, number,
                             file_size, match_meta, filename):
        def _insert(status, referenceDay, video_path, number,
                    file_size, match_meta, filename):
            sql = '''INSERT INTO referenceMeta
                        SET status=%s,video_date=%s,video_path=%s,
                        number=%s,file_size=%s,match_meta=%s,filename=%s'''
            yield db_insert(sql, status, referenceDay, video_path, number,
                            file_size, match_meta, filename)

        def _update(status, referenceDay, video_path, number,
                    file_size, match_meta, filename):
            sql = '''UPDATE referenceMeta SET status=%s,video_date=%s,
                        number=%s,file_size=%s,match_meta=%s,filename=%s
                        WHERE video_path=%s'''
            yield db_execute(sql, status, referenceDay, number,
                             file_size, match_meta, filename, video_path)
        if referenceDay and number and \
                self.is_referenceMeta_video_exists(referenceDay, number):
            db_txn(self.db_pool, partial(_update, status, referenceDay,
                                         video_path, number, file_size,
                                         match_meta, filename))
        else:
            db_txn(self.db_pool, partial(_insert, status, referenceDay,
                                         video_path, number, file_size,
                                         match_meta, filename))

    @Retry(10512000, exceptions=(OperationalError,), delay=3, logger=logger)
    def fetch_no_match_url(self, day):
        def _fetch(day):
            sql = '''SELECT id,file_md5,video_path,feature_dir,url_date FROM url
                    WHERE status='query_success' AND isnull(match_meta) AND
                            url_date<=%s
                    ORDER BY url_date LIMIT 1 '''
            rc, rs = yield db_query(sql, day)
            yield db_result(rc, rs)
        _, res = db_txn(self.db_pool, partial(_fetch, day))
        return res

    @Retry(10512000, exceptions=(OperationalError,), delay=3, logger=logger)
    def update_no_match_url(self, md5, match_meta):
        def _update(md5, match_meta):
            sql = '''UPDATE url SET match_meta=%s WHERE file_md5=%s'''
            yield db_execute(sql, match_meta, md5)
        db_txn(self.db_pool, partial(_update, md5, match_meta))


if __name__ == '__main__':
    access = AccessDB('mysql://root:1234@192.168.6.193:3306/comScore')
    access.insert_referenceDay(
        '20160908', 8, 'false', '2016-08-08 12:12:12', '2016-08-08 22:12:12')
    # print access.is_referenceDay_exists('20160909')
    # print access.is_referenceDay_exists('20160908')
    # access.insert_referenceMeta(
    #     'new', '20160908', '/a/b/c.mpr', 8, 555,
    #     's78d0f_dfsdfs', '3434sdfsdfsadfa.mp4')
    # res = access.fetch_no_match_url('20160909')
    # print res
    # if res:
    #     access.update_no_match_url(res[0][0], 'abcefg')
    #     print 'is_referenceMeta_video_exists'
    # print access.is_referenceMeta_video_exists(
    #     '20160919', '0a80a0ceb5665bfcc9b98288c85bbeb8.f4v')
