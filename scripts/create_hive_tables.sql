CREATE EXTERNAL TABLE published_history (
    `_id` STRING, `_status` INT, `_order` INT,
    `author_id` STRING, `author_name` STRING, `author_org` STRING,
    `paper_id` STRING, `paper_title` STRING, `year` FLOAT
) 
STORED AS PARQUET
LOCATION 's3://recsys-bucket-1/data_lake/arnet/tables/published_history/merge-0/'
TBLPROPERTIES ('external.table.purge'='false');
