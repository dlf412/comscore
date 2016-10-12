#!/usr/bin/env python
# encoding: utf-8
log_level = "INFO"
# log_level = "DEBUG"
config_down_ftp = {'host': 'comscore.ftp.vobile.net',
                   'user': 'comScore', 'passwd': 'cS@aDVb16'}
config_upload_ftp = {'host': 'ftp.comscore.com',
                     'user': 'Vobile', 'passwd': 'k49d77h3'}
config_match_days = -3
config_day_time = '07:00'
config_check_ftp_interval = 20 * 60
# config_check_ftp_interval = 3
config_check_ftp_frequency = 6
# config_check_ftp_frequency = 2
config_check_day_interval = 10 * 60
# config_check_day_interval = 3
config_vobile_email = ['zeng_ruige@vobile.cn']
config_comscore_email = ['zeng_ruige@vobile.cn']
config_fail_max_count = 200
# config_fail_max_count = 0
config_fail_max_percent = 2
