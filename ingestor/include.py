#!/usr/bin/env python
# encoding: utf-8
import sys
import os
from os.path import abspath, join, dirname
from config import log_level
PAR_DIR = abspath(join(dirname(__file__), os.pardir))
sys.path.append(join(PAR_DIR, 'lib'))
from mwlogger import ALogger
logger = ALogger('ingestor', 'syslog')
logger.setLevel(log_level)
video_search = join(PAR_DIR, 'tools', 'video_search')
