#!/usr/bin/env python
# encoding: utf-8

import os
import gzip
import shutil
import traceback

import pysftp

from functools import partial
from os.path import dirname, exists, basename
from utils import md5sum


class FTPSyncError(Exception):
    pass


class FileNotExists(FTPSyncError):
    pass


class CompressError(FTPSyncError):
    pass


class InvalidFilelist(FTPSyncError):
    pass


class MD5CheckError(FTPSyncError):
    pass


class DownloadError(FTPSyncError):
    pass


class UploadError(FTPSyncError):
    pass


class ConnectionError(FTPSyncError):
    pass


class FTPSync(object):
    conn = None

    def __init__(self, host, username=None, password=None, port=22):
        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        try:
            self.conn = pysftp.Connection(host,
                                          username=username,
                                          password=password,
                                          port=port, cnopts=cnopts)
        except:
            raise ConnectionError("connection failed, %s" %
                                  traceback.format_exc())

    def download_d(self, remotedir, localdir):
        try:
            self.conn.get_d(remotedir, localdir)
        except:
            raise DownloadError("download error, error: %s" %
                                traceback.format_exc())

    def download(self, remotefile, localfile, files):
        try:
            if type(files) == str:
                files = [files]
            for f in files:
                self.conn.get(os.path.join(remotefile, basename(f)),
                              os.path.join(localfile, basename(f)))
        except:
            raise DownloadError("download error, error: %s" %
                                traceback.format_exc())

    def upload(self, localdir, remotedir, files):
        try:
            if type(files) == str:
                files = [files]
            if not self.conn.exists(remotedir):
                self.conn.makedirs(remotedir)
            for f in files:
                self.conn.put(os.path.join(localdir, basename(f)),
                              os.path.join(remotedir, basename(f)))
        except:
            raise UploadError("upload error, error:%s" %
                              traceback.format_exc())

    def exists(self, src):
        return self.conn.exists(src)

    def __del__(self):
        if self.conn:
            self.conn.close()


def _remote_path(root_path, day):
    _1st = root_path
    _2nd = day[:4] + '_' + day[4:6]
    _3rd = day
    return os.path.join(_1st, _2nd, _3rd)


def _output_files(dailydir, suffix='.csv'):
    #NOTES, call once `listdir` can not find file !!!
    # we use cloudfs, after first access, it will sync file from oss server
    csvs = os.listdir(dailydir)
    csvs = os.listdir(dailydir)
    #NOTES
    endswith = lambda suff, self: str.endswith(self, suff)
    csvs = filter(partial(endswith, suffix), csvs)
    return [os.path.join(dailydir, c) for c in csvs]


def _filelist(dailydir, files):
    fname = os.path.join(dailydir, "filelist.txt")
    with open(fname, 'w') as fd:
        for f in files:
            fd.write("%s    %s\n" % (basename(f), md5sum(f)))
    return fname


def gz(src, files, dst=None):
    if dst is None:
        dst = src
    for f in files:
        with open(os.path.join(src, f), 'r') as f_in, \
                gzip.open(os.path.join(dst, f + ".gz"), 'wb') as f_out:
            f_out.writelines(f_in)
    return [os.path.join(dst, f + ".gz") for f in files]


def ungz(src, files, dst=None):
    if dst is None:
        dst = src
    for f in files:
        with gzip.open(os.path.join(src, f), 'r') as f_in, \
                open(os.path.join(dst, f[:-3]), 'wb') as f_out:
            f_out.writelines(f_in)
    return [os.path.join(dst, f[:-3]) for f in files]


def valid_files(dailydir, filelist):
    files = []
    with open(os.path.join(dailydir, filelist), "r") as f:
        for line in f:
            if not line.strip():
                continue
            file_name, md5 = line.strip().split()
            abs_file = os.path.join(dailydir, file_name)
            if not exists(abs_file):
                raise InvalidFilelist("%s not exist" % file_name)
            if not md5sum(abs_file) == md5:
                raise MD5CheckError("md5 check failed: %s" % (abs_file))
            files.append(abs_file)
    return files


def daily_download(ftpsync, day, localdir, uncompress=False):
    '''
    download sample url file list by one day
    '''
    indir = _remote_path('DailyInput', day)
    dailydir = os.path.join(localdir, indir)
    if not exists(dailydir):
        os.makedirs(dailydir)
    if not ftpsync.exists(os.path.join(indir, 'filelist.txt')):
        raise FileNotExists("filelist.txt not exists in {%s}" % indir)
    ftpsync.download_d(indir, dailydir)
    files = valid_files(dailydir, "filelist.txt")
    if not files:
        raise FileNotExists("filelist.txt is empty, in {%s}" % indir)
    if uncompress:
        files = ungz(dailydir, [basename(f) for f in files])
    return files


def daily_upload(ftpsync, day, localdir, compress=True):
    '''
    upload results by one day
    '''
    outdir = _remote_path('DailyReturn', day)
    dailydir = os.path.join(localdir, outdir)
    suffix = '.csv.gz' if compress else '.csv'
    outfiles = _output_files(dailydir, suffix=suffix)

    if not compress:
        outfiles = gz(dailydir, outfiles)

    ftpsync.upload(dailydir, outdir, outfiles)
    filelist = _filelist(dailydir, outfiles)
    ftpsync.upload(dailydir, outdir, filelist)

    return outfiles


def reference_download(ftpsync, day, localdir):
    '''
    download reference video
    '''
    indir = _remote_path('ReferenceCreative', day)
    download_dir = localdir
    filelist = 'filelist.txt'
    if not ftpsync.exists(os.path.join(indir, filelist)):
        raise FileNotExists("filelist.txt not exists in {%s}" % indir)
    if not exists(download_dir):
        os.makedirs(download_dir)

    ftpsync.download(indir, download_dir, filelist)

    reference_info = []
    md5_error_file = []
    invalid_file = []
    with open(os.path.join(download_dir, filelist), "r") as f:
        for line in f:
            video = line.strip()
            if video:
                reference_info.append(video)
                meta = os.path.join(indir, video)
                if not ftpsync.exists(meta):
                    invalid_file.append(video)
                else:
                    local_meta = os.path.join(localdir, video)
                    if not exists(local_meta) or \
                            md5sum(local_meta) != video[:32]:
                        ftpsync.download(indir, download_dir, [local_meta])
                    if md5sum(local_meta) != video[:32]:
                        md5_error_file.append(video)
    if not reference_info:
        raise FileNotExists("filelist.txt is empty, on {}".format(day))
    return (reference_info, md5_error_file, invalid_file)


def ingest_upload(ftpsync, day, localdir):
    '''
    ingested csv file upload,
    filelist.txt as last uploaded file
    '''
    outdir = _remote_path('AdsIngestion', day)
    upload_dir = os.path.join(localdir, outdir)
    result_files = _output_files(upload_dir, suffix='.csv.gz')
    filelist = _filelist(upload_dir, result_files)
    ftpsync.upload(upload_dir, outdir, result_files)
    ftpsync.upload(upload_dir, outdir, filelist)

    return result_files


def test_download():
    ftpsync = FTPSync('ftp.comscore.com',
                      username='Vobile',
                      password='k49d77h3')
    day = "20160801"
    localdir = "/tmp"
    print daily_download(ftpsync, day, localdir, uncompress=True)

# test_download()


def test_upload():
    ftpsync = FTPSync('ftp.comscore.com',
                      username='Vobile',
                      password='k49d77h3')
    day = "20160823"
    localdir = "/tmp"
    print daily_upload(ftpsync, day, localdir, compress=False)
# test_upload()


def test_reference_download():
    ftpsync = FTPSync('ftp.comscore.com',
                      username='Vobile',
                      password='k49d77h3')
    day = "20160801"
    localdir = "/tmp"
    print reference_download(ftpsync, day, localdir)

# test_reference_download()


def test_ingest_upload():
    ftpsync = FTPSync('ftp.comscore.com',
                      username='Vobile',
                      password='k49d77h3')
    day = "20160801"
    localdir = "/tmp"
    print ingest_upload(ftpsync, day, localdir)
