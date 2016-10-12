FETCH_SAME_TASK = '''SELECT id,file_md5,video_path
                      FROM url
                      WHERE DATEDIFF('{date}', url_date) <= {max_days}
                      AND file_name='{name}' AND domain='{domain}'
                      AND brother_id=id
                      LIMIT 1'''
UPDATE_SAME_TASK = '''UPDATE url
                       SET status='download_success',
                            file_md5='{md5}',brother_id={bid},video_path='{vpath}'
                       WHERE id={id}'''
DOWNLOAD_SUCCESS = '''UPDATE url
                       SET status='download_success',brother_id=id,
                            file_md5='{md5}',download_count=download_count+1,
                            file_size={len},download_speed={speed},video_path='{vpath}'
                       WHERE id={id}'''
DOWNLOAD_FAIL = '''UPDATE url
                    SET status='download_failed',
                         file_size={file_size},
                         download_count=download_count+1
                    WHERE id={id}'''
