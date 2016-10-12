#! /usr/bin/env python


import os
import re
import sys
import uuid
import hashlib
import commands
from pipes import quote
from os.path import join, exists, basename, dirname

class CreateDnaError(Exception): pass
class IngestDnaError(Exception): pass
class QueryDnaError(Exception): pass
class RedisConnectError(Exception): pass


def _execute(video_search, video, key, frame, ac, host='', port='', password='',
             match_threshold=''):
    cmd = "{video_search} {video} {key} {frame} {ac} {host} {port} \
            {password} {match_threshold}".format(
            video_search=quote(video_search), video=quote(video), key=quote(key),
            frame=quote(frame), ac=ac, host=host, port=port, password=password,
            match_threshold=match_threshold)
    ret, res = commands.getstatusoutput(cmd)
    return ret>>8, res

def create_dna(video_search, video, md5, feature_dir):
    ref = "-".join([md5[:8], md5[8:12], md5[12:16], md5[16:20], md5[20:]])
    key = join(feature_dir, ref)
    frame = join(feature_dir, "frame")
    if exists(key) and exists(frame):
        return key, frame
    if not exists(video):
        raise CreateDnaError("video: %s not exists" % video)
    if not exists(feature_dir):
        os.makedirs(feature_dir)
    ret, res = _execute(video_search, video, key, frame, 'create_dna')
    if ret:
        raise CreateDnaError(res)
    if not exists(frame) or not exists(key):
        raise CreateDnaError("gen frame or key failed" + res)
    return key, frame

def ingest(video_search, video, key, frame, host, port, password):
    ret, res = _execute(video_search, video, key, frame, 'insert_dna',
                        host, port, password)
    if ret == 201:
        raise RedisConnectError(res)
    elif ret:
        raise IngestDnaError(res)
    return basename(key)

def query(video_search, video, key, frame, host, port, password,
          match_threshold="22 22"):
    ret, res = _execute(video_search, video, key, frame, 'query_dna',
                        host, port, password, match_threshold)
    if ret == 201:
        raise RedisConnectError(res)
    elif ret and ret != 202:
        raise QueryDnaError(res)
    if "video_name" not in res:
        return None
    return re.search("video_name\s=\s(.*)", res).group(1)

if __name__ == "__main__":

    video = "/home/media_wise/ads_test/5a00f9744440ead0c6a0d1fed3f3283a.mp4"
    match_video = "/home/media_wise/ads_test/5a00f9744440ead0c6a0d1fed3f3283a.mp4"
    no_match_video = "/home/media_wise/ads_test/1b6c6524ef6c16ee9bc26e1780ae6f9a.mp4"

    data = "/home/media_wise/xxx"
    video_search = "/home/media_wise/ads_checker/video_search"
    host = "127.0.0.1"
    port = 8200
    password = "123456"

    #key, frame = create_dna(video_search, video, data)
    #print key, frame
    #key = "/home/media_wise/ads_checker/sample/5a00f9744440ead0c6a0d1fed3f3283a.mp4/06839e9a-6d97-11e6-a5c4-fa163e62d49e"
    #frame = "/home/media_wise/ads_checker/sample/5a00f9744440ead0c6a0d1fed3f3283a.mp4/frame"
    #res = query(video_search, video, key, frame, host, port, password)
    #print res

    #print ingest(video_search, video, key, frame, host, port, password)

    #print query(video_search, video, key, frame, host, port, password)
    #print query(video_search, no_match_video, key, frame, host, port, password)

