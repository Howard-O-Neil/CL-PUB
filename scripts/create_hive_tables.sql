CREATE EXTERNAL TABLE published_history (
    `_id` STRING, `_status` INT, `_order` INT,
    `author_id` STRING, `author_name` STRING, `author_org` STRING,
    `paper_id` STRING, `paper_title` STRING, `year` FLOAT
) 
STORED AS PARQUET
LOCATION 's3://recsys-bucket-1/data_lake/arnet/tables/published_history/merge-0/'
TBLPROPERTIES ('external.table.purge'='false');

CREATE EXTERNAL TABLE coauthor (
    `_id` STRING, `_status` INT, `_order` INT,
    `paper_id` STRING, `paper_title` STRING,
    `author1_id` STRING, `author1_name` STRING, `author1_org` STRING,
    `author2_id` STRING, `author2_name` STRING, `author2_org` STRING,
    `year` FLOAT
)
STORED AS PARQUET
LOCATION 's3://recsys-bucket-1/data_lake/arnet/tables/coauthor/merge-0'
TBLPROPERTIES ('external.table.purge'='false');
