log_level = "INFO"
max_task_count = 1000
poll_db_interval = 100
config_max_downloading = 10000
mq_queue = "download_retrier_queue"
mq_routing_key = "download_retrier_routing_key"
mq_exchange = "download_retrier_exchange"
max_file_size = 52428800
config_domains = ['youku.com', 'ykimg.com', 'tudou.com',
                  'tudouui.com', 'tdimg.com', 'le.com',
                  'letv.com', 'letvcdn.com', 'iqiyi.com',
                  'qiyi.com', 'sohu.com', 'qq.com',
                  'qzoneapp.com', 'gtimg.com']
config_fetch_day = -10
# config_dates = "AND url_date>='20160827' AND url_date<='20160829'"
config_dates = ""
