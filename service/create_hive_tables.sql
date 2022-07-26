CREATE EXTERNAL TABLE published_history (
    `_id` STRING, `_status` INT, `_order` INT,
    `author_id` STRING, `author_name` STRING, `author_org` STRING,
    `paper_id` STRING, `paper_title` STRING, `year` FLOAT
) 
STORED AS PARQUET
LOCATION 'gs://clpub/data_lake/arnet/tables/published_history/merge-0/'
TBLPROPERTIES ('external.table.purge'='false');

CREATE EXTERNAL TABLE coauthor (
    `_id` STRING, `_status` INT, `_order` INT,
    `paper_id` STRING, `paper_title` STRING,
    `author1_id` STRING, `author1_name` STRING, `author1_org` STRING,
    `author2_id` STRING, `author2_name` STRING, `author2_org` STRING,
    `year` FLOAT
)
STORED AS PARQUET
LOCATION 'gs://clpub/data_lake/arnet/tables/coauthor/merge-0'
TBLPROPERTIES ('external.table.purge'='false');

CREATE EXTERNAL TABLE author_feature (
    `author_id` STRING, `author_name` STRING, 
    `feature` STRING, `ranking` FLOAT, `org_rank` FLOAT
)
STORED AS PARQUET
LOCATION 'gs://clpub/data_lake/arnet/tables/user_feature/re4/merge-0'
TBLPROPERTIES ('external.table.purge'='false');

CREATE EXTERNAL TABLE paper_info (
    `_id` STRING, `_status` INT, `_order` INT,
    `paper_id` STRING, `title` STRING, `abstract` STRING,
    `authors_id` ARRAY<STRING>, `authors_name` ARRAY<STRING>, `authors_org` ARRAY<STRING>,
    `year` FLOAT, `venue_raw` STRING, `issn` STRING, `isbn` STRING, `doi` STRING,
    `pdf` STRING, `url` ARRAY<STRING>
)
STORED AS PARQUET
LOCATION 'gs://clpub/data_lake/arnet/tables/papers/merge-0'
TBLPROPERTIES ('external.table.purge'='false');
