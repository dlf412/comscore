DOWNLOADING_COUNT = '''SELECT COUNT(*) as c
                     FROM url
                     WHERE status='downloading' AND is_valid_url='true' AND
                      download_count > 0 AND
                      file_size <= {file_size} {domains} {dates} {big_dates}'''
NEWLY_DATE = '''SELECT url_date FROM url ORDER BY url_date DESC LIMIT 1'''
LOAD_TASKS = '''SELECT id,url,domain,file_name,url_date,csv_file,
                        csv_file_number,download_count,updated_at
                 FROM url
                 WHERE status='download_failed' AND is_valid_url='true' AND
                    file_size <= {file_size} {domains} {dates} {big_dates}
                 ORDER BY download_count,url_date
                 LIMIT {limit}'''
UPDATE_STATUS = '''UPDATE url
                    SET status='downloading'
                    WHERE updated_at <= '{updated_at}' AND id in ({ids})'''
