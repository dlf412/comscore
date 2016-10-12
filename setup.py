#!/usr/bin/env python
# encoding: utf-8

'''
Usage:
  setup.py <module> --dir=INSTALL_DIR [--config=MW_CONFIG]
                      [--autostart] [--upgrade|--rollback]
  setup.py (--version|--help)

Arguments:

  module: mw_pusher/mw_repusher/all

Options:

  -h --help                 show this help message and exit
  --version                 show version and exit
  -d --dir INSTALL_DIR      set the install dir
  -c --config MW_CONFIG     set the mediawise config file
  -a --autostart            the module autostart after setup it
  -u --upgrade              Upgrade the program
  -r --rollback             Rollback the program
'''
from os import remove, makedirs, listdir
from os.path import join, exists, isfile
import sys
import commands
import shutil
from docopt import docopt


LAST_VERSION = "LAST_VERSION"


def get_version(install_dir):
    version = commands.getoutput("cat PROG_VERSION.def | cut -f2 -d=").strip()
    if not version:
        version = "bak"
    return version


def install(module, install_dir):
    ret, out = commands.getstatusoutput(
        "cp -R {} {}".format("./", install_dir))
    if ret:
        print >> sys.stderr, "Copy the module files to install dir Failed!"
        exit(out)
    if "all" not in module:
        for del_module in ["distributor", "downloader", "download_retrier",
                           "querier", "query_retrier", "result_collector",
                           "transfer"]:
            if del_module not in module:
                ret, out = commands.getstatusoutput(
                    "rm -r {}".format(join(install_dir, del_module)))
                if ret:
                    print >> sys.stderr, "Remove {} Failed! {}".format(
                        del_module, out)
    if not exists(join(install_dir, LAST_VERSION)):
        ret, out = commands.getstatusoutput(
            "mkdir {}".format(join(install_dir, LAST_VERSION)))
        if ret:
            print >> sys.stderr, "mkdir LAST_VERSION dir Failed!"
            exit(out)
        else:
            print "mkdir {} OK".format(join(install_dir, LAST_VERSION))
        ret, out = commands.getstatusoutput(
            "cp -R {} {}".format(
                "./", join(install_dir, LAST_VERSION)))
        if ret:
            print >> sys.stderr, "Copy the module files to "\
                                 "LAST_VERSION dir Failed!"
            exit(out)


def clear_all_dir(clear_dir):
    for file in listdir(clear_dir):
        if isfile(join(clear_dir, file)):
            remove(join(clear_dir, file))
        else:
            shutil.rmtree(join(clear_dir, file))


def back_all_old(install_dir):
    if exists(join(install_dir, LAST_VERSION)):
        clear_all_dir(join(install_dir, LAST_VERSION))
        print "backup all old files({})".format(
            open(join(install_dir, 'PROG_VERSION.def')).read().strip('\n'))
        for file in listdir(install_dir):
            if isfile(join(install_dir, file)):
                ret, out = commands.getstatusoutput(
                    "cp {} {}".format(join(install_dir, file),
                                      join(install_dir, LAST_VERSION)))
                if ret:
                    print >> sys.stderr, "Copy all old files"\
                                         " to LAST_VERSION dir Failed!"
                    exit(out)
            else:
                if LAST_VERSION not in file:
                    ret, out = commands.getstatusoutput(
                        "cp -R {} {}".format(join(install_dir, file),
                                             join(install_dir, LAST_VERSION)))
                    if ret:
                        print >> sys.stderr, "Copy all old dir"\
                                             " to LAST_VERSION dir Failed!"
                        exit(out)


def rollback_etc(install_dir):
    print "rollback etc files"
    ret, out = commands.getstatusoutput(
        "cp {}/* {}".format(
            join(install_dir, LAST_VERSION, 'etc'),
            join(install_dir, 'etc')))
    if ret:
        print >> sys.stderr, "Copy etc from LAST_VERSION dir Failed!"
        exit(out)


def clear_excpet_back_dir(install_dir):
    print "clear excpet back dir"
    for file in listdir(install_dir):
        if isfile(join(install_dir, file)):
            remove(join(install_dir, file))
        else:
            if LAST_VERSION not in file:
                shutil.rmtree(join(install_dir, file))


def stop_server(install_dir):
    ret, out = commands.getstatusoutput(
        "supervisorctl -c {} shutdown".format(
            join(install_dir, "etc", "supervisor.conf")))
    print ret, out


def main():
    args = docopt(__doc__, version="v0.1")
    # print args
    # return
    '''
    {'--autostart': False,
    '--config': None,
    '--dir': './',
    '--help': False,
    '--version': False,
    '<module>': 'repusher'}
    '''
    module = args['<module>']
    # mw_config = args['--config']
    # autostart = args['--autostart']
    install_dir = args['--dir']

    if module not in ("distributor", "downloader", "download_retrier",
                      "querier", "query_retrier", "result_collector",
                      "transfer", "all"):
        exit("""<module> must be in ("distributor", "downloader", "download_retrier",
                      "querier", "query_retrier", "result_collector",
                      "transfer", "all")""")

    # copy the module to install_dir

    if args["--upgrade"]:
        # upgrade
        last_version_file = join(install_dir, LAST_VERSION)
        if not exists(install_dir) or \
                not exists(last_version_file):
            print "No old version or upgraded!"
            pass
        else:
            # stop the server
            stop_server(install_dir)
            back_all_old(install_dir)
            # remove all files except dir LAST_VERSION
            clear_excpet_back_dir(install_dir)
            install(module, install_dir)
            # recover etc files
            rollback_etc(install_dir)
            print "Upgrade OK!({})".format(
                open(join(install_dir, 'PROG_VERSION.def')).read().strip('\n'))

    elif args["--rollback"]:
        # rollback
        # read backup version
        last_version = join(install_dir, LAST_VERSION)
        if not exists(last_version):
            pass
        else:
            # stop the server
            stop_server(install_dir)
            clear_excpet_back_dir(install_dir)
            # copy from LAST_VERSION
            ret, out = commands.getstatusoutput(
                "cp -R {}/* {}".format(
                    join(install_dir, LAST_VERSION), install_dir))
            if ret:
                print >> sys.stderr, "Copy the all files from LAST_VERSION "\
                                     "to install dir Failed!"
                exit(out)
            print "Rollback OK!({})".format(
                open(join(install_dir, 'PROG_VERSION.def')).read().strip('\n'))

    else:
        back_all_old(install_dir)
        install(module, install_dir)
        print "Install OK!({})".format(
            open(join(install_dir, 'PROG_VERSION.def')).read().strip('\n'))

    # remove old supervisord.sock
    old_sock_path = join(install_dir, 'var', 'run', 'supervisord.sock')
    if exists(old_sock_path):
        remove(old_sock_path)

    # if mw_config is not None:
    #     ret, out = commands.getstatusoutput(
    #         'python tools/config_update.py "{}"'.format(mw_config))
    #     if ret:
    #         print >> sys.stderr, "Run config_update.py Failed"
    #         exit(out)

    # start supervisord using etc/supervisor.conf
    log_dir = join(install_dir, "var", "log")
    run_dir = join(install_dir, "var", "run")
    # etc_file = join(install_dir, "etc", "supervisor.conf")
    if not exists(log_dir):
        makedirs(log_dir)
    if not exists(run_dir):
        makedirs(run_dir)
    # ret, out = commands.getstatusoutput("supervisord -c {}".format(etc_file))
    # error_desc = "Error: Another program is already listening on a port that "\
    #              "one of our HTTP servers is configured to use."
    # if ret and error_desc not in out:
    #     print >> sys.stderr, "Run supervisord Failed"
    #     exit(out)

    # if autostart:
    #     # TODO: supervisorctl start the module
    #     ret, out = commands.getstatusoutput(
    #         "supervisorctl -c {} restart {}".format(etc_file, module))
    #     if "ERROR" in out:
    #         print >> sys.stderr, "start {} Failed".format(module)
    #         exit(out)

if __name__ == "__main__":
    main()
