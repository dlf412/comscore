#!/usr/bin/env python
# encoding: utf-8
__author__ = 'deng_lingfei'

import requests
import json
import traceback

def send(subject, body, recv, url="http://cm.ops.vobile.org/api/sendEmail"):
    if url and recv and subject:
        _tos = ','.join(recv)
        data = '&'.join(["tos=%s" % _tos, "subject=%s" % subject,
                         "content=%s" % body.replace('\n', '<br/>')])
        try:
            r = requests.post(url, data=data,
                              headers={"Content-Type": "application/x-www-form-urlencoded"})

            if r.status_code != 200:
                print "request mail service failed, code=%d, errormsg is %s" % (
                    r.status_code, r.text)
            else:
                res = json.loads(r.text)
                if not res['success']:
                    print "send mail failed, errormsg is %s" % res['message']
        except:
            traceback.print_exc()
