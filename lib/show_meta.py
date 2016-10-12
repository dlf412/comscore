#!/usr/bin/python
import json
import ssl
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    # Legacy Python that doesn't verify HTTPS certificates by default
    pass
else:
    # Handle target environment that doesn't support HTTPS verification
    ssl._create_default_https_context = _create_unverified_https_context
import httplib
from httplib import HTTPException


SC_OK = 200
SC_NOT_FOUND = 404

class AuthError(Exception): pass
class GetMetaError(Exception): pass

class meta_shower(object):

    def __init__(self, host, port, user, password, meta_uuid):
        self.host = host
        self.port = int(port)
        self.user = user
        self.password = password
        self.meta_uuid = meta_uuid
        self.conn = httplib.HTTPSConnection(self.host, self.port)

    def auth(self):
        request = {"protocols":["5.1"], "user":self.user,
                   "password":self.password}
        self.conn.request ('POST', '/mediawise/auth', json.dumps(request,
                            ensure_ascii=False, encoding='utf-8'))
        response = self.conn.getresponse()
        res = response.read()
        if response.status == SC_OK:
            return response.getheader('set-cookie')
        else:
            raise AuthError("host:%s, port:%s, user:%s, password:%s" %
                    (self.host, self.port, self.user, "***"))

    def get(self):
        token = self.auth()
        header =  {'Content-type':'application/x-www-form-urlencoded',
                   'Cookie':token}
        self.conn.request("GET", '/mediawise/contents/%s/meta' %
                          self.meta_uuid, None, header)
        response = self.conn.getresponse()
        results = response.read()
        if response.status == SC_OK:
            results = json.loads(results)
            return results["body"][0]
        elif response.status == SC_NOT_FOUND:
            try:
                json.loads(results)
            except:
                raise GetMetaError("get meta info failed, address:%s, port:%s, "
                                   "username:%s, password:%s, code:%s, msg:%s"
                                   % (self.host, self.port, self.user,
                                      "***", response.status, results))
            return {}
        else:
            raise GetMetaError("get meta info failed, address:%s, port:%s, "
                               "username:%s, password:%s, code:%s, msg:%s"
                               % (self.host, self.port, self.user,
                                  "***", response.status, results))


if __name__ == '__main__':
    m = meta_shower("192.168.1.10", 443, "zhang_jin", "123",
            "53a6b0fc-6cc2-11e6-b781-00e04dc34651")
    print str(m.get()["duration"])
