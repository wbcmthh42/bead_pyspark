CREATE EXTERNAL TABLE IF NOT EXISTS reddit_sg.sample

(
`id` STRING,
`timestamp` TIMESTAMP,
`author` STRING,
`body` STRING

)

PARTITIONED BY (`dt` date)

ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
LOCATION 's3://bead-reddit-sg/isfwqm/'
TBLPROPERTIES('parquet.compression'='GZIP');
