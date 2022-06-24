Don't try to config spark inside the ETL script
Do this only
    spark = (pyspark.sql.SparkSession.builder.getOrCreate())

The script will be run in server provider's spark shell
    The config depends on provider
