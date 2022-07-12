from pyspark.sql import SparkSession

data = [
    ("arthur", "helen", 1),
    ("helen", "arthur", 1),
    ("arthur", "alex", 1),
    ("alex", "arthur", 1),
    ("alex", "alexa", 1),
    ("alexa", "alex", 1),
    ("davie", "homles", 1),
    ("homles", "davie", 1),
]

spark = SparkSession.builder.getOrCreate()

df = spark.createDataFrame(data, ["name1", "name2", "pos"])
df.createOrReplaceTempView("coauth_df")

unique_author = spark.sql("""
    select distinct list_author.name as name
    from (
        (select distinct name1 as name
        from coauth_df
        where name1 != "")
        union
        (select distinct name2 as name
        from coauth_df
        where name2 != "")
    ) as list_author
""")
unique_author.createOrReplaceTempView("unique_df")

cross_join = spark.sql("""
    select ud1.name as name1, ud2.name as name2
    from unique_df as ud1 cross join unique_df as ud2
    where ud1.name != ud2.name
""")
cross_join.createOrReplaceTempView("cross_join_df")

negative_sampling_1 = spark.sql("""
    select *
    from cross_join_df
    where (name1, name2) not in (
        select name1, name2 from coauth_df
    )
""")
negative_sampling_1.show()

negative_sampling_2 = spark.sql("""
    select cjd.name1, cjd.name2
    from cross_join_df cjd
        left join coauth_df cd on cd.name1 = cjd.name1 and cd.name2 = cjd.name2
    where cd.name1 is null and cd.name2 is null
""")
negative_sampling_2.show()

