#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
   Copyright (c) 2016 Vobile Inc.  All Rights Reserved.
   Author: xu_xiaorong
   Email: xu_xiaorong@mycompany.cn
   Created_at: 2016-08-10 13:46:10
'''

LOG_HANDLER = None
LOG_LEVEL = 'INFO'

FETCH_COUNT = 100
FETCH_INTERVAL = 5
TASK_ACTIVE_TIME = 60*86400

QUEUE_NAME = "querier_queue"
QUEUE_EXCHANGE = "querier_exchange"
QUEUE_ROUTING_KEY = "querier_routing_key"
