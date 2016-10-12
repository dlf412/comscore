#!/usr/bin/env python
# encoding: utf-8
from include import logger
from sendmail import send as send_mail

subject_create_dna_error = '[comScore项目] 多个视频形式广告无法提取特征 {}'
body_create_dna_error = '''项目：comScore
文件日期：{}
问题：当日客户上传的视频广告中，多段视频文件无法正确提取特征值，比例超过系统设定的阈值，请人工审核该批视频。
无法提取特征值的视频如下：
{}'''

subject_md5_error = '[comScore项目] 客户提供的视频形式广告MD5不一致 {}'
body_md5_error = '''项目：comScore
文件日期：{}
问题：当日客户上传的视频广告中，下列视频在Filelist中记录的MD5值与文件本身的MD5值不一致，请人工确认，如有问题请通知客户重新上传。
MD5值不一致的视频如下：
{}{}'''
body_md5_error_others = '''
另外，在filelist.txt中存在，但是在ftp上面找不到的视频如下:
{}'''

subject_invalid_file = '[comScore项目] 客户提供的视频形式广告不存在 {}'
body_invalid_file = '''项目：comScore
文件日期：{}
问题：当日客户上传的视频广告中，下列视频在Filelist中有记录，但是在ftp上面不存在，请人工确认，如有问题请通知客户重新上传。
在ftp上面找不到的视频如下：
{}'''

subject_not_found_filelist = '[Error]找不到视频形式广告的Filelist {}'
body_not_found_filelist = '''项目：comScore
文件日期：{}
问题：未找到当日视频形式广告的Filelist，请人工确认客户是否上传了视频形式广告。
'''

subject_successfully = '[Success]Ad Creative Files Has Been Processed Successfully {}'
body_successfully = '''Hi,

We have successfully processed the ad creative files uploaded on {} and we have uploaded the ingestion result CSV file to your FTP.

Please login to the FTP to find the matched file.

{}
Yours sincerely,
Vobile MediaWise Team'''
body_successfully_unsupported = '''The following videos are either corrupted or with unsupported format, thus we’re unable to process. Ingestion result of these videos are labeled “Fail”.
{}
'''


class EmailDeliver(object):
    def __init__(self, config_comscore_email, config_mycompany_email):
        self.mail_comscore = config_comscore_email
        self.mail_mycompany = config_mycompany_email

    def send_md5_fail(self, day, md5_error_file, invalid_file):
        not_exists_files = ''
        if invalid_file:
            not_exists_files = \
                body_md5_error_others.format('\n'.join(invalid_file))
        subject = subject_md5_error.format(day)
        body = body_md5_error.format(
            day, '\n'.join(md5_error_file), not_exists_files)
        logger.info('send md5 fail files email to {}'.format(self.mail_mycompany))
        send_mail(subject, body, self.mail_mycompany)

    def send_invalid_file(self, day, invalid_file):
        subject = subject_invalid_file.format(day)
        body = body_invalid_file.format(day, '\n'.join(invalid_file))
        logger.info('send invalid files email to {}'.format(self.mail_mycompany))
        send_mail(subject, body, self.mail_mycompany)

    def send_filelist_miss(self, day):
        subject = subject_not_found_filelist.format(day)
        body = body_not_found_filelist.format(day)
        logger.info('send filelist miss email to {}'.format(self.mail_mycompany))
        send_mail(subject, body, self.mail_mycompany)

    def send_fail(self, day, error_files):
        subject = subject_create_dna_error.format(day)
        body = body_create_dna_error.format(day, '\n'.join(error_files))
        logger.info('send make dna fail email to {}'.format(self.mail_mycompany))
        send_mail(subject, body, self.mail_mycompany)

    def send_success(self, day, problem_files):
        error_file_info = ''
        if problem_files:
            error_file_info = body_successfully_unsupported.format(
                '\n'.join(problem_files))
        subject = subject_successfully.format(day)
        body = body_successfully.format(day, error_file_info)
        logger.info('send success email to {}'.format(
            self.mail_comscore + self.mail_mycompany))
        send_mail(subject, body, self.mail_mycompany)
        send_mail(subject, body, self.mail_comscore)
