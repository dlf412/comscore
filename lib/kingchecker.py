#!/usr/bin/python

'''
    King(master) biding utility
'''

import MySQLdb
import urllib
import traceback
import time
import sys
import threading

__all__ = [
        'Kingchecker'
        ]

class Kingchecker(threading.Thread):
    '''
        Kingchecker can check if itself is king
    '''
    def __init__(self, module_name, module_host, module_port, master_timeout, check_interval=5, db_url=None, logger=None,
                **kwargs):
        super(Kingchecker, self).__init__()
        self.module_name = module_name
        self.module_host = module_host
        self.module_port = module_port
        self.master_timeout = master_timeout
        self.db_url = db_url
        self.db_args = kwargs
        self.check_interval = check_interval #seconds
        self.conn = None
        self.setDaemon(True)
        self.logger = logger

    def check_util_king(self):
        while True:
            try:
                if self._is_king() is False:
                    self.logger.info("check and wait be master")
                    time.sleep(self.check_interval)
                    continue
                else:
                    break
            except Exception, e:
                self.logger.error("check_until_king - unhandled error occurred - {0}".format(e))
                self.logger.error(traceback.format_exc())

    def run(self):
        while True:
            try:
                if self._is_king() is False:
                    self.logger.info('run - find from master to slave. process exit!')
                    sys.exit(-1)
            except Exception, e:
                self.logger.error("run - unhandled error occurred - {0}".format(e))
                self.logger.error(traceback.format_exc())
            finally:
                time.sleep(self.check_interval)

    def __del__(self):
        if self.conn:
            self.conn.close()

    def _is_king(self):
        '''
        Try biding for master through database.

        First, assume itself to be master and try update the record. If there is
        record updated, then it is master, else proceeding.
        Second, check whether or not record with `module_name' ever exists. If yes,
        then it is not master for sure, else proceeding.
        Third, try insert a record of `module_name' with `module_host' and
        `module_port', and it becomes master.

        True is returned if biding succeeded, False otherwise. False is also
        returned on any exception raised.
        '''
        try:
            if self.conn == None:
                if self.db_url:  # Use db_url if specified
                    self.conn = connect(db_url=self.db_url)
                else:
                    self.conn = MySQLdb.connect(**(self.db_args))

            curs = self.conn.cursor()
            # If we are king now, send a heartbeat
            sql = '''
UPDATE masterProcess
   SET heartbeat_time = NOW(), host = %s, port = %s
 WHERE module_name = %s AND
       ((host = %s AND port = %s) OR
         heartbeat_time < NOW() - INTERVAL %s SECOND)
'''
            sql_params = (self.module_host, self.module_port, self.module_name, self.module_host,
                          self.module_port, self.master_timeout)
            ret = curs.execute(sql, sql_params)
            self.conn.commit()
            if ret > 0:
                return True

            # Else check if the row ever exists
            sql = 'SELECT id FROM masterProcess WHERE module_name = %s'
            sql_params = (self.module_name, )
            ret = curs.execute(sql, sql_params)
            if ret > 0:
                return False
            # Otherwise, insert the row to be king!
            sql = '''
INSERT INTO masterProcess(module_name, host, port, heartbeat_time, created_at,
                          updated_at)
VALUES (%s, %s, %s, NOW(), NOW(), NOW())
'''
            sql_params = (self.module_name, self.module_host, self.module_port)
            curs.execute(sql, sql_params)
            self.conn.commit()
            return True
        except Exception, e:  # False is returned is exception raised
            self.logger.error('_is_king - unhandled error occurred - {0}'.format(e))
            return False
        finally:
            time.sleep(1)


def connect(db_url=None, **kwargs):
    '''
    Connect database whether using a db_url, or normal host-port params.
    An connection is returned on success, otherwise exception is raised

    Examples:
        conn = connect(db_url='mysql://vdna:123456@127.0.0.1/mddb_local')
      or
        conn = connect(host='127.0.0.1', port=3306, user='vdna',
                       passwd='123456', db='mddb_local', use_unicode=True)
    '''
    if db_url:
        proto, user, passwd, host, port, db, \
                table = parse_db_url(db_url, default_port=3306)
        if proto != 'mysql':
            raise Exception('protocol not supported - {0}'.format(proto))
        return MySQLdb.connect(host=host, port=port, user=user, passwd=passwd,
                               db=db, use_unicode=True)
    else:
        if 'use_unicode' not in kwargs:   # If not specified, use_unicode is
            kwargs['use_unicode'] = True  # enabled by default
        return MySQLdb.connect(**kwargs)

def execute(sql, sql_params=None, curs=None, conn=None, commit=False, **kwargs):
    '''
    Execute on sql on the specified cursor `curs', or connection `conn', or
    create a new connection using `kwargs'.

    Examples:
        conn = connect('mysql://vdna:123456@127.0.0.1/mddb_local')
        ret1, res1 = execute(sql1, sql_params1, conn=conn)
        ret2, res2 = execute(sql2, sql_params2, conn=conn)
        ...
        conn.commit()
      or
        ret, res = execute(sql, sql_params, commit=True,
                           db_url='mysql://vdna:123456@127.0.0.1/mddb_local')
      or
        db_conf = {'host': '127.0.0.1', 'port': 3306, 'user': 'vdna',
                   'passwd': '123456', 'db': 'mddb_local'}
        ret, res = execute(sql, sql_params, commit=True, **db_conf)
    '''
    curt = curs
    cont = conn
    if not curt:
        if not cont:
            cont = connect(**kwargs)
        curt = cont.cursor()
    ret = curt.execute(sql, sql_params)
    res = curt.fetchall()
    if commit:
        try:
            cont.commit()
        except:
            cont.rollback()
            raise
    # Internally created connection, close when done
    if not curs and not conn:
        cont.close()

    return ret, res

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

    return proto, user, passwd, host, port, db, table

def parse_url(url, default_port=None):
    '''
    Parse url in the following form:
      PROTO://[USER:[:PASSWD]@]HOST[:PORT][/PATH[;ATTR][?QUERY]]
    A tuple containing (proto, user, passwd, host, port, path, tag, attrs, query) is returned,
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

