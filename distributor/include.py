#!/usr/bin/env python
# encoding: utf-8
import sys
import os
from os.path import abspath, join, dirname
PAR_DIR = abspath(join(dirname(__file__), os.pardir))
sys.path.append(join(PAR_DIR, 'lib'))
