
CREATE EXTERNAL TABLE `raw`(
  `author` string, 
  `body` string, 
  `created_utc` timestamp, 
  `distinguished` string, 
  `id` string, 
  `is_submitter` boolean, 
  `link_id` string, 
  `score` int, 
  `stickied` boolean, 
  `submission` string, 
  `subreddit` string, 
  `subreddit_id` string, 
  `sub_author` string, 
  `sub_created_utc` timestamp, 
  `sub_id` string, 
  `sub_link_flair_text` string, 
  `sub_num_comments` int, 
  `sub_score` int, 
  `sub_title` string, 
  `sub_upvote_ratio` double, 
  `sub_url` string)
PARTITIONED BY ( 
  `sr` string, 
  `sm` string, 
  `dt` date)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://bead-reddit-all/raw_sr_sm_dt'
TBLPROPERTIES (
  'parquet.compression'='GZIP', 
  'transient_lastDdlTime'='1710743946')