LOAD_TASKS = '''SELECT id,url,domain,file_name,url_date,csv_file, csv_file_number
                 FROM url
                 WHERE status='new' AND is_valid_url='true' {domains} {dates}
                 ORDER BY url_date
                 LIMIT {limit}'''
UPDATE_STATUS = '''UPDATE url
                    SET status='downloading'
                    WHERE status='new' AND id in ({ids})'''
