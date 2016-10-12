#!/usr/bin/env python
# encoding: utf-8
__author__ = 'deng_lingfei'

import traceback

from config import (ftp_setting, local_dir, notify_customer_time)
import database as dao
from myutils import (json_cfg, logger, notify_ourselves, date_offset_today,
                     is_over_time, now, ftp_sync, csv_import,
                     filelist_error_notify, md5_error_notify)


def download(ftpobj, day):
    info = dao.DailyInfo(day, local_dir)
    try:
        info.files = ftp_sync.daily_download(
            ftpobj, day, local_dir, uncompress=False)
        info.status = 'download_ok'
    except ftp_sync.MD5CheckError as err:
        logger.warn("dailyInput[{}] {}".format(day, err))
        if day >= date_offset_today(-1):  # notify only one day
            md5_error_notify(day)
    except ftp_sync.InvalidFilelist as err:
        logger.warning("dailyInput[{}] {}".format(day, err))
        notify_ourselves(
            "[alarm]dailyInput[{}] maybe error".format(day), str(err))
    except ftp_sync.FileNotExists as err:
        if is_over_time(notify_customer_time):
            logger.warn("dailyInput[{}] {}".format(day, err))
            if day >= date_offset_today(-1):  # notify only one day
                filelist_error_notify(day)
    except ftp_sync.FTPSyncError as err:
        logger.error(str(err))
        notify_ourselves("[alarm]DailyInput[{}] FtpSyncError".format(day),
                         str(err))
    except:
        logger.error(traceback.format_exc())
        notify_ourselves("[alarm]DailyInput Downlaod Unhandle Error",
                         traceback.format_exc())
    finally:
        info.dl_end = now()
    return info


def importdb(dbpool, dailyinfo):
    dailyinfo.im_start = now()
    for dfile in list(set(dailyinfo.files) - set(dailyinfo.im_files)):
        try:
            logger.info("start import file:{}...".format(dfile))
            urlreader = csv_import.URLReader(dfile, url_date=dailyinfo.daily)
            urlreader.save2db(dbpool)
            dailyinfo.im_files.append(dfile)
        except:
            logger.error(traceback.format_exc())
            notify_ourselves("[alarm]DailyInput Import Error",
                             traceback.format_exc())
    if dailyinfo.im_files != dailyinfo.files:
        dailyinfo.status = 'import_err'
    else:
        dailyinfo.status = 'import_ok'
    dailyinfo.im_end = now()
    return dailyinfo


def process(ftpobj, dbpool, day):
    logger.info("start download daily[{}]...".format(day))
    dailyinfo = download(ftpobj, day)
    logger.info(str(dailyinfo))
    if not dailyinfo.files:
        return
    dao.saveDailyInfo(dbpool, dailyinfo)
    logger.info("start import daily[{}]".format(dailyinfo.daily))
    dailyinfo = importdb(dbpool, dailyinfo)
    logger.info(str(dailyinfo))
    dao.saveDailyInfo(dbpool, dailyinfo)

if __name__ == '__main__':

    try:
        dbpool = csv_import.make_pool(json_cfg.work_db)
        agos = dao.getDailyInfos(dbpool, ago=7)
        logger.info("load 7 days ago dailyInput info:{}".format(agos))
        re_imports = filter(lambda a: a.status != 'import_ok', agos)
        logger.info("re-import {} dailys".format(len(re_imports)))
        for re in re_imports:
            logger.info("re-import dailyinfo:[{}]".format(str(re)))
            dailyinfo = importdb(dbpool, re)
            logger.info(str(dailyinfo))
            dao.saveDailyInfo(dbpool, dailyinfo)

        logger.info("load 7 days ago new dailyInfo")
        news = dao.dailyinfoNews(dbpool, ago=7)
        if not news:
            logger.info("no new daily to process now. I exit")
            exit(0)
        logger.info("need to process dailys:{}".format(news))

        logger.info("connect to ftpServer:{}".format(ftp_setting.host))
        ftpsync = ftp_sync.FTPSync(ftp_setting.host,
                                   ftp_setting.user,
                                   ftp_setting.password)
        logger.info("connect ftpServer successfully")

        for day in news:
            process(ftpsync, dbpool, day)

    except SystemExit:
        exit(0)

    except:
        notify_ourselves("[alarm]DailyInput Unhandle Error",
                         traceback.format_exc())
        logger.error(traceback.format_exc())
        raise
