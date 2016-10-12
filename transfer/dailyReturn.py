#!/usr/bin/env python
# encoding: utf-8
__author__ = 'deng_lingfei'

import traceback

from config import (results_delivered_agos, results_delivered_time,
                    ftp_setting, local_dir)
import database as dao
from myutils import (json_cfg, logger, notify_ourselves, is_over_time,
                     now, csv_export, ftp_sync, success_notify,
                     date_offset_today)


def process(ftpobj, dbpool, day):
    if not is_over_time(results_delivered_time):
        logger.info("Now is earlier than results_delivered_time[{}], "
                    "do nothing".format(results_delivered_time))
        return

    dailyresult = export(dbpool, day)
    logger.info(str(dailyresult))
    if dailyresult.status == 'export_err':
        return

    if not dailyresult.files:
        logger.info("daily[{}] dose not have any result,"
                    " maybe DailyInput is empty".format(day))
        return
    dao.saveDailyResult(dbpool, dailyresult)
    dailyresult = upload(ftpobj, dailyresult)
    logger.info(str(dailyresult))
    dao.saveDailyResult(dbpool, dailyresult)
    if dailyresult.status == 'upload_ok':
        success_notify(day)


def upload(ftpsync, result):
    result.ul_start = now()
    try:
        result.ul_files = ftp_sync.daily_upload(ftpsync,
                                                result.daily,
                                                local_dir,
                                                compress=True)
        result.status = 'upload_ok'
    except Exception as err:
        logger.error("upload {} results error: {}".format(
            result.daily, str(err)))
        notify_ourselves("[alarm]{} DailyReturn Upload Error".format(result.daily),
                         traceback.format_exc())
        result.status = 'upload_err'
    finally:
        result.ul_end = now()
    return result


def export(dbpool, day):
    result = dao.DailyResult(day, local_dir)
    try:
        r = csv_export.Results(dbpool, day)
        result.files = r.dump2csv(local_dir)
        result.status = 'export_ok'
    except:
        logger.error(traceback.format_exc())
        notify_ourselves("[alarm]{} DailyReturn Export Error".format(result.daily),
                         traceback.format_exc())
        result.status = 'export_err'
    finally:
        result.ex_end = now()
    return result


if __name__ == '__main__':

    try:
        dbpool = csv_export.make_pool(json_cfg.work_db)

        logger.info("connect to ftpServer:{}".format(ftp_setting.host))
        ftpsync = ftp_sync.FTPSync(ftp_setting.host,
                                   ftp_setting.user,
                                   ftp_setting.password)
        logger.info("connect ftpServer successfully")

        agos = dao.getDailyResults(dbpool, results_delivered_agos)
        logger.info("load {} days ago dailyResult:{}".format(
            results_delivered_agos, agos))
        re_uploads = filter(lambda a: a.status ==
                            'export_ok' or a.status == 'upload_err', agos)

        for re in re_uploads:
            logger.info("start upload dailyResult: {}".format(str(re)))
            result = upload(ftpsync, re)
            logger.info(str(result))
            dao.saveDailyResult(dbpool, result)
            if re.status == 'upload_ok':
                success_notify(re.daily)

        logger.info("load {} days ago new dailyResults".format(
            results_delivered_agos))
        news = dao.dailyresultNews(dbpool, results_delivered_agos)
        if not news:
            logger.info("no new daily to process now. I exit")
            exit(0)
        logger.info("need to process dailys:{}".format(news))

        for day in news:
            logger.info("start process daily[{}]".format(day))
            process(ftpsync, dbpool, day)

    except SystemExit:
        exit(0)

    except:
        notify_ourselves("DailyReturn UnHandle Error", traceback.format_exc())
        logger.error(traceback.format_exc())
        raise
