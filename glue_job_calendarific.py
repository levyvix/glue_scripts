import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as f

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET_NAME = 'dataeng-clean-zone-957'

holidays_dynamicframe = glueContext.create_dynamic_frame.from_catalog(
    database="holidays", table_name="holidays"
)

holidays_dataframe = holidays_dynamicframe.toDF()

holidays_dataframe_transformed = (
    holidays_dataframe
    .withColumn('`date.iso`', f.to_timestamp(f.col('`date.iso`')))
    .withColumn('type', f.regexp_extract(f.col('type'), "(\w+)", 1))
)

holidays_dataframe_clean = (
    holidays_dataframe_transformed
    .selectExpr(
        'name',
        'description',
        'type',
        'primary_type',
        'canonical_url',
        'urlid',
        'locations',
        'states',
        '`country.id` as country_id',
        '`country.name` as country_name',
        '`date.iso` as date_iso',
        '`date.datetime.year` as year',
        '`date.datetime.month` as month',
        '`date.datetime.day` as day',
        '`date.datetime.hour` as hour',
        '`date.datetime.minute` as minute',
        '`date.timezone.offset` as timezone_offset',
        '`date.timezone.zoneabb` as timezone_zoneabb',
        '`date.timezone.zoneoffset` as timezone_zone_offset'
    )
)

holidays_dataframe_clean.write.format('parquet').mode('overwrite').save(f's3://{BUCKET_NAME}/holidays')


job.commit()
