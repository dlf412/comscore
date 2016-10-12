#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
   Copyright (c) 2016 Vobile Inc.  All Rights Reserved.
   Author: xu_xiaorong
   Email: xu_xiaorong@vobile.cn
   Created_at: 2016-08-10 13:46:10
'''

LOG_HANDLER = None # None means stdout, syslog means syslog
LOG_LEVEL = 'INFO'

QUEUE_NAME = "querier_queue"
QUEUE_EXCHANGE = "querier_exchange"
QUEUE_ROUTING_KEY = "querier_routing_key"

MATCH_THRESHOLD = "22 22" #"sample reference"
