log_level = "INFO"
max_task_count = 1000
poll_db_interval = 100
mq_queue = "downloader_queue"
mq_routing_key = "downloader_routing_key"
mq_exchange = "downloader_exchange"
config_domains = ['youku.com', 'ykimg.com', 'tudou.com',
                  'tudouui.com', 'tdimg.com', 'le.com',
                  'letv.com', 'letvcdn.com', 'iqiyi.com',
                  'qiyi.com', 'sohu.com', 'qq.com',
                  'qzoneapp.com', 'gtimg.com']
# config_dates = "AND url_date>='20160827' AND url_date<='20160829'"
config_dates = ""
