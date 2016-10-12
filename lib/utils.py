import sys
import urllib
import hashlib
import MySQLdb
import threading
import subprocess
import signal
import commands
from functools import partial
from os import _exit, getpid
from socket import getfqdn
from time import sleep
from DBUtils.PersistentDB import PersistentDB

from kingchecker import Kingchecker
from dbpc import dbpc
from mwlogger import MwLogger


def md5sum(filename, blocksize=65536):
    hash = hashlib.md5()
    with open(filename, "rb") as f:
        for block in iter(lambda: f.read(blocksize), b""):
            hash.update(block)
    return hash.hexdigest()


def catch_and_die():
    def catcher(f):
        def new_f(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except:
                logger = MwLogger('catch_and_die')
                logger.error('function call fails with ' +
                             'exception', exc_info=True)
                _exit(1)
        return new_f
    return catcher


def ignore_exception(name):
    def catch(f):
        def wrap(*args, **kwargs):
            try:
                return f(*args, **kwargs)
            except:
                logger = MwLogger('ignore_exception: ' + name)
                logger.error("called failed", exc_info=True)
        return wrap
    return catch


def parse_url(url, default_port=None):
    '''
    Parse url in the following form:
      PROTO://[USER:[:PASSWD]@]HOST[:PORT][/PATH[;ATTR][?QUERY]]
    A tuple containing (proto, user, passwd, host, port,
        path, tag, attrs, query) is returned,
    where `attrs' is a tuple containing ('attr1=value1', 'attr2=value2', ...)
    '''
    proto, user, passwd, host, port, path, tag, attrs, query = (None, ) * 9

    try:
        proto, tmp_host = urllib.splittype(url)
        tmp_host, tmp_path = urllib.splithost(tmp_host)
        tmp_user, tmp_host = urllib.splituser(tmp_host)
        if tmp_user:
            user, passwd = urllib.splitpasswd(tmp_user)
        host, port = urllib.splitport(tmp_host)
        port = int(port) if port else default_port
        tmp_path, query = urllib.splitquery(tmp_path)
        tmp_path, attrs = urllib.splitattr(tmp_path)
        path, tag = urllib.splittag(tmp_path)
    except Exception, err:
        raise Exception('parse_db_url error - {0}'.format(str(err)))

    return proto, user, passwd, host, port, path, tag, attrs, query


def parse_db_url(db_url, default_port=None):
    '''
    Parse an url representation of one database settings.
    The `db_url' is in the following form:
      PROTO://[USER[:PASSWD]@]HOST[:PORT][/DB/TABLE]
    Tuple (proto, user, passwd, host, port, db, table) is returned
    '''
    proto, user, passwd, host, port, db, table = (None, ) * 7

    try:
        proto, user, passwd, host, port, path = parse_url(db_url,
                                                          default_port)[0:6]
        if not passwd:
            passwd = ''
        tmp_list = path.split('/')[1:]
        db, table = '', ''
        if len(tmp_list) >= 2:
            db, table = tmp_list[0:2]
        elif len(tmp_list) == 1:
            db = tmp_list[0]
    except Exception, err:
        raise Exception('parse_db_url error - {0}'.format(str(err)))

    return proto, str(user), str(passwd), str(host), port, str(db), str(table)


def make_conn(db_url, *args, **kwargs):
    _, user, passwd, host, port, db, _ = parse_db_url(db_url)
    conn = MySQLdb.connect(host=host,
                           port=port,
                           user=user,
                           passwd=passwd,
                           db=db,
                           charset='utf8',
                           use_unicode=False)
    cur = conn.cursor()
    cur.execute('set time_zone="+0:00"')
    cur.close()
    conn.commit()
    return conn


def make_pool(db_url):
    return PersistentDB(creator=partial(make_conn, db_url))


def popen(cmd):
    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    ret = proc.wait()
    out, err = proc.communicate()
    return ret, out, err


def start_dbpc(config, module_name, logger, heartbeat=60):
    '''
        construct a dbpc object then start a new thread
    '''
    dbpc_sender = dbpc(config['host'], int(config['port']),
                       config['service'],
                       config['component_prefix'] + module_name,
                       logger, heartbeat)
    dbpc_sender.start()
    return dbpc_sender


def start_kingchecker(db_url, module_name, timeout, logger):
    '''
        start_kingcheck include two step,
        1.block current thread until it become master
        2.once a certain time interval to check, if found itself not master,
          then exit current process
        #NOTE, this fun should called after start_dbpc if needed
    '''
    host = getfqdn()
    port = getpid()
    _, user, passwd, host, port, db, _ = parse_db_url(db_url)
    king_checker = Kingchecker(module_name, host, port,
                               int(timeout), host=host,
                               port=port, user=user,
                               passwd=passwd, db=db, logger=logger)
    king_checker.check_util_king()
    king_checker.start()


class Retry(object):
    default_exceptions = (Exception,)

    def __init__(self, tries, exceptions=None, delay=0, logger=None):
        """
        Decorator for retrying function if exception occurs
        tries -- num tries
        exceptions -- exceptions to catch
        delay -- wait between retries
        """
        self.tries = tries
        if exceptions is None:
            exceptions = Retry.default_exceptions
        self.exceptions = exceptions
        self.delay = delay
        self.logger = logger

    def __call__(self, f):
        def fn(*args, **kwargs):
            for i in range(self.tries):
                try:
                    return f(*args, **kwargs)
                except self.exceptions, e:
                    if self.logger:
                        self.logger.error(str(e))
                    else:
                        print >> sys.stderr, str(e)
                    if (i + 1) < self.tries:
                        sleep(self.delay)
                    exception = e
            # if no success after tries, raise last exception
            raise exception
        return fn


def timeout_and_die():
    logger = MwLogger('timeout_and_die')

    def decorator(func):
        def _handle_timeout(name):
            logger.error('called ' + name + ' timeout, just exit!')
            _exit(1)

        def wrapper(*args, **kwargs):
            if kwargs.has_key('limit'):
                seconds = kwargs['limit']
            else:
                seconds = 600
            logger.debug("seconds:%s, kwargs:%s", seconds, kwargs)
            timer = threading.Timer(
                seconds, _handle_timeout, args=[func.__name__])
            timer.start()
            try:
                result = func(*args, **kwargs)
            finally:
                timer.cancel()
            return result

        return wrapper

    return decorator


def trans2utf8(data):
    str_body = {key.encode('utf-8') if isinstance(key, unicode) else key:
                value.encode('utf-8') if isinstance(value, unicode) else value
                for key, value in data.items()}
    return str_body


class CoreDumpError(Exception):
    pass


def wrap_cmd(cmd):
    ret, out = commands.getstatusoutput(cmd)
    ret = ret >> 8
    if (ret - 128) in (signal.SIGQUIT, signal.SIGILL, signal.SIGABRT,
                       signal.SIGFPE, signal.SIGSEGV):
        raise CoreDumpError("cmd:%s, ret:%s, out:%s", cmd, ret, out)
    return ret, out
