import sys

sys.path.append("../../..")

from pprint import pprint
import numpy as np
import pandas as pd

# each JSON is small, there's no need in iterative processing
import json
import sys
import os
import xml
import time

import pyspark
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

from prototype.crawl_init_data.increase_id import cal_next_id

# import requests
# response = requests.get("https://dblp.org/search/author/api?q=Tin Huynh&format=json")
# print(json.dumps(response.json(), indent=4))

# exit()

os.environ["JAVA_HOME"] = "/opt/corretto-8"
os.environ["HADOOP_CONF_DIR"] = "/recsys/prototype/spark_submit/hdfs_cfg"
os.environ["HADOOP_USER_NAME"] = "hadoop"

JAVA_LIB = "/opt/corretto-8/lib"

spark = SparkSession.builder \
    .config("spark.app.name", "Recsys") \
    .config("spark.master", "yarn") \
    .config("spark.submit.deployMode", "client") \
    .config("spark.yarn.appMasterEnv.SPARK_HOME", "/opt/spark") \
    .config("spark.yarn.appMasterEnv.PYSPARK_PYTHON", "/virtual/python/bin/python") \
    .config("spark.yarn.jars", "hdfs://128.0.5.3:9000/lib/java/spark/jars/*.jar") \
    .getOrCreate()

# In case you have deleted all hdfs files associates to the author table
# Run this to clear cached
spark.sql("""
REFRESH TABLE author
""")

# Create a schema for inserting only. No update, delete on database side
# Each item id have multiple record, select the latest
# If the latest "normal", keep it
# If the latest "deleted", skip it

# Int 0 -> normal
# Int 1 -> deleted
spark.sql("""
CREATE TABLE IF NOT EXISTS author (_id LONG, _status INT, _timestamp LONG, id STRING, name STRING, urls ARRAY<STRING>, affiliations ARRAY<STRING>)
    USING PARQUET
    LOCATION "hdfs://128.0.5.3:9000/data/recsys/dblp/tables/author/test/";
""")

count_id = -1

for i in range(5):
    count_id += 1
    spark.sql(f"""
    INSERT INTO author VALUES
        ({count_id}, 0, {int(time.time())}, 'notanid1', 'San Jose', array('123', '123'), array('123', '456'));
    """)

for i in range(3):
    count_id += 1
    spark.sql(f"""
    INSERT INTO author VALUES
        ({count_id}, 0, {int(time.time())}, 'notanid2', 'Jose Ju', array('123', '123'), array('123', '456'));
    """)
count_id += 1
spark.sql(f"""
INSERT INTO author VALUES
    ({count_id}, 1, {int(time.time())}, 'notanid2', 'Jose Ju', array('123', '123'), array('123', '456'));
""")

for i in range(5):
    count_id += 1
    spark.sql(f"""
    INSERT INTO author VALUES
        ({count_id}, 0, {int(time.time())}, 'notanid3', 'Ju Jankin', array('123', '123'), array('123', '456'));
    """)

for i in range(7):
    count_id += 1
    spark.sql(f"""
    INSERT INTO author VALUES
        ({count_id}, 0, {int(time.time())}, 'notanid4', 'Jankin Seoul', array('123', '123'), array('123', '456'));
    """)
count_id += 1
spark.sql(f"""
INSERT INTO author VALUES
    ({count_id}, 1, {int(time.time())}, 'notanid4', 'Jankin Seoul', array('123', '123'), array('123', '456'));
""")

count_id += 1
spark.sql(f"""
INSERT INTO author VALUES
    ({count_id}, 1, {int(time.time())}, 'notanid4', 'Jankin Seoul', array(), array());
""")
count_id += 1
spark.sql(f"""
INSERT INTO author VALUES
    ({count_id}, 0, {int(time.time())}, 'notanid4', 'Jankin Seoul', array(), array());
""")

string_id = 'notanid4'
count_id += 1
spark.sql(f"""
INSERT INTO author VALUES
    ({count_id}, 0, {int(time.time())}, '{string_id}', 'Jankin Seoul', array(), array());
""")

count_id += 1
spark.sql(f"""
INSERT INTO author VALUES
    ({count_id}, 0, {int(time.time())}, '{string_id}', "Jankin' Seoul", array(), array());
""")

count_id += 1
spark.sql(f"""
INSERT INTO author VALUES
    ({count_id}, 0, {int(time.time())}, '{string_id}', 'Jankin\" Seoul', array(), array());
""")

spark.sql("""    
select * from author
order by _id desc
""").show(100)


# =====
# Make a query to filter out usable data from author data lake
# =====

# Solution 1
spark.sql("""
with t(id, _id, _status) as (
    select a.id, a._id, a._status
    from author as a INNER JOIN (select a2._id, row_number() over (
                                    distribute by a2.id 
                                    order by a2._id desc) as row
                                from author a2) as cou ON cou._id = a._id
    where a._id IS NOT NULL AND a._status IS NOT NULL 
        AND a._timestamp IS NOT NULL 
        AND cou.row = 1
)
select * from author a
where EXISTS (
    select t._id from t
    where t._status = 0 AND t._id = a._id
);
""").show()

# Solution 2 (The same as INNER JOIN, but join use where)
spark.sql("""
with t(id, _id, _status) as (
    select a.id, a._id, a._status
    from author as a, (select a2._id, row_number() over (
                            distribute by a2.id 
                            order by a2._id desc) as row
                        from author a2) as cou
    where cou._id = a._id
        AND a._id IS NOT NULL AND a._status IS NOT NULL 
        AND a._timestamp IS NOT NULL 
        AND cou.row = 1
)
select * from author a
where EXISTS (
    select t._id from t
    where t._status = 0 AND t._id = a._id
);
""").show()

# Solution 3 (Use IN or EXISTS will do)
spark.sql("""
with t(id, _id, _status) as (
    select a.id, a._id, a._status
    from author as a INNER JOIN (select a2._id, row_number() over (
                                    distribute by a2.id 
                                    order by a2._id desc) as row
                                from author a2) as cou ON cou._id = a._id
    where a._id IS NOT NULL AND a._status IS NOT NULL 
        AND a._timestamp IS NOT NULL 
        AND cou.row = 1
)
select * from author a
where a._id IN (
    select t._id from t
    where t._status = 0
);
""").show()

# Solution 4 (Use Join)
spark.sql("""
with t(_id, id, name) as (
    select u._id, u.id, u.name
    from author u, (
        select a.id, a._id, a._status
        from author as a INNER JOIN (select a2._id, row_number() over (
                                        distribute by a2.id 
                                        order by a2._id desc) as row
                                    from author a2) as cou ON cou._id = a._id
        where a._id IS NOT NULL AND a._status IS NOT NULL 
            AND a._timestamp IS NOT NULL 
            AND cou.row = 1
    ) t
    where u._id = t._id and t._status = 0
)
select * from t
""").show()


# Solution 5
spark.sql("""
with t(id, _id, _status) as (
    select a.id, a._id, a._status
    from author as a
    where a._id IS NOT NULL AND a._status IS NOT NULL 
        AND a._timestamp IS NOT NULL
        AND 1 = (select first_value(a3.row)
                from (select a2._id, row_number() over (
                            distribute by a2.id 
                            order by a2._timestamp desc) as row
                        from author a2) as a3
                where a3._id = a._id) 
)
select * from author a 
where EXISTS (
    select t._id from t
    where t._status = 0 AND t._id = a._id
);
""").show()

# Solution 6
spark.sql("""
with t(_id, _status) as (
    select a._id, a._status
    from author as a
    where a._id IS NOT NULL AND a._status IS NOT NULL 
        AND a._timestamp IS NOT NULL
    group by a._id, a._status
    having a._id In
        (select a3._id
        from (select a2._id, row_number() over (
                    distribute by a2.id
                    order by a2._timestamp desc) as row
                from author a2) as a3
        where a3.row = 1)
)
select * from author a 
where EXISTS (
    select t._id from t
    where t._status = 0 AND t._id = a._id
);
""").show()

# Some testing

spark = SparkSession.builder \
    .config("spark.app.name", "Recsys") \
    .config("spark.master", "local[*]") \
    .config("spark.submit.deployMode", "client") \
    .getOrCreate()

testDF = spark.createDataFrame([
    ('Jose Ju', 12, 15), 
    ('Jose Ju', 12, 17),
    ('Jose Ju', 14, 15),
    ('Jose Ju', 13, 17),
    ('Layna King', 57, 58),
    ('Layna King', 57, 59),
    ('Layna King', 59, 32)
], ["name", "rec1", "rec2"])

testDF.createOrReplaceTempView('people')

spark.sql("""
select name, rec1, max(name)
from people
group by name, rec1
""").show()

# Develop utils functions
def check_available_name(name):
    name = name.replace('"','\\"')
    res = spark.sql(f"""
            select * from author a
            where a.name = "{name}"
        """).collect() \

    print(res)
    return len(res) > 0

check_available_name("Jankin' Seoul")
check_available_name("Jankin\" Seoul")

def get_latest_id():
    res = spark.sql(f"""
            select a._id from author a
            order by a._id desc
            limit 1
        """).collect() \

    res = list(map(lambda x: x.asDict(), res))
    if len(res) == 0:
        return 0
    return int(res[0]["_id"])

def check_available_uuid(uuid):
    res = spark.sql(f"""
            select * from author a
            where a.id = '{uuid}'
        """).collect() \

    return len(res) > 0

spark.sql("""
select * from author a
order by a._id desc
""").show(100)

get_latest_id()
check_available_name("Ju Jankin")
check_available_uuid("notanid5")