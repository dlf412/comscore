log_level = "DEBUG"
same_file_name_max_days = 60
mq_queue = "downloader_queue"
mq_routing_key = "downloader_routing_key"
mq_exchange = "downloader_exchange"
re_mq_queue = "download_retrier_queue"
re_mq_routing_key = "download_retrier_routing_key"
re_mq_exchange = "download_retrier_exchange"
querier_mq_queue = "querier_queue"
querier_mq_routing_key = "querier_routing_key"
querier_mq_exchange = "querier_exchange"
WGET_COMMAND = """timeout 900 wget {url} -O {file} -o {log} --tries=3 --connect-timeout=10 --read-timeout=3 -c"""
config_file_name_len = 10
config_download_count = 5
config_video_type = [r'\.mp4', r'\.f4v', r'\.flv', r'\.m4a', r'\.ogg',
                     r'\.mp3', r'\.wav', r'\.swf', '']
config_re_find = r'(.*/.+?{})\?'
