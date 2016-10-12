CREATE TABLE `url` (
     `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
     `ad_idc` varchar(128) NOT NULL,
     `file_name` varchar(256) NOT NULL,
     `url` varchar(2048) NOT NULL,
     `url_date` date NOT NULL DEFAULT '0000-00-00',
     `domain` varchar(256) NOT NULL,
     `file_md5` char(36) DEFAULT NULL,
     `csv_file` varchar(128) NOT NULL,
     `csv_file_number` int(5) unsigned NOT NULL DEFAULT '0' COMMENT 'url line number in csv file',
     `brother_id` int(10) unsigned DEFAULT '0',
     `status` enum('new','downloading','download_failed','download_success','query','query_success','query_failed') NOT NULL DEFAULT 'new',
     `error_code` smallint(6) unsigned DEFAULT '0',
     `is_valid_url` enum('false','true') NOT NULL DEFAULT 'true',
     `is_valid_file` enum('false','true') NOT NULL DEFAULT 'true',
     `is_ingested` enum('false','true') NOT NULL DEFAULT 'false',
     `download_speed` smallint(6) unsigned DEFAULT '0',
     `download_count` smallint(6) unsigned DEFAULT '0',
     `file_size` int(10) unsigned DEFAULT '0',
     `duration` smallint(6) unsigned DEFAULT '0',
     `video_path` varchar(256) DEFAULT NULL,
     `feature_dir` varchar(256) DEFAULT NULL,
     `query_count` smallint(6) unsigned DEFAULT '0',
     `match_meta` char(36) DEFAULT NULL,
     `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
     `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     PRIMARY KEY (`id`),
     UNIQUE KEY `idx_ad_idc` (`ad_idc`),
     KEY `idx_filename_domain` (`file_name`(255),`domain`(255)),
     KEY `idx_urldate` (`url_date`),
     KEY `idx_md5` (`file_md5`),
     KEY `idx_match_meta` (`match_meta`)
);

CREATE TABLE referenceMeta (
     id int(10) unsigned NOT NULL AUTO_INCREMENT,
     status enum('new','dna_ok','dna_err','md5_err','no_exists','insert_ok') NOT NULL DEFAULT 'new',
     video_date char(8) CHARACTER SET utf8 DEFAULT NULL,
     video_path varchar(256) CHARACTER SET utf8 NOT NULL,
     number int(10) unsigned NOT NULL DEFAULT '0',
     file_size int(10) unsigned DEFAULT '0',
     match_meta char(36) CHARACTER SET utf8 DEFAULT NULL,
     created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
     updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     filename varchar(50) CHARACTER SET utf8 NOT NULL DEFAULT '',
     PRIMARY KEY (id),
     KEY idx_video_date (video_date),
     KEY idx_match_meta (match_meta),
     KEY idx_number (number)
);

CREATE TABLE referenceDay (
     id int(10) unsigned NOT NULL AUTO_INCREMENT,
     video_date char(8) CHARACTER SET utf8 NOT NULL,
     num int(10) unsigned NOT NULL,
     status enum('false','true') DEFAULT 'false',
     start timestamp DEFAULT '0000-00-00 00:00:00',
     end timestamp DEFAULT '0000-00-00 00:00:00',
     created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
     updated_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     PRIMARY KEY (id),
     KEY idx_video_date (video_date)
);

CREATE TABLE `dailyInput` (
     `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
     `daily` char(8) NOT NULL,
     `files` varchar(512) DEFAULT NULL,
     `status` enum('new','download_ok','import_ok','download_err','import_err') NOT NULL DEFAULT 'new',
     `im_files` varchar(512) DEFAULT NULL,
     `dl_start` int(10) unsigned DEFAULT '0',
     `dl_end` int(10) unsigned DEFAULT '0',
     `im_start` int(10) unsigned DEFAULT '0',
     `im_end` int(10) unsigned DEFAULT '0',
     `local_dir` varchar(128) NOT NULL,
     `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
     `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     PRIMARY KEY (`id`),
     UNIQUE KEY `idx_daily` (`daily`)
);

CREATE TABLE `dailyReturn` (
     `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
     `daily` char(8) NOT NULL,
     `files` varchar(512) DEFAULT NULL,
     `status` enum('new','export_ok','upload_ok','export_err','upload_err') NOT NULL DEFAULT 'new',
     `ul_files` varchar(512) DEFAULT NULL,
     `ex_start` int(10) unsigned DEFAULT '0',
     `ex_end` int(10) unsigned DEFAULT '0',
     `ul_start` int(10) unsigned DEFAULT '0',
     `ul_end` int(10) unsigned DEFAULT '0',
     `local_dir` varchar(128) NOT NULL,
     `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
     `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
     PRIMARY KEY (`id`),
     UNIQUE KEY `idx_daily` (`daily`)
);
