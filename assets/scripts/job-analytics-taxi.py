import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ['JOB_NAME','database','table','databucket'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

table=str(args['table'])
databucket= str(args['databucket'])
database=str(args['database'])


# Script generated for node taxidata
taxidata_node1659731970404 = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name="tripdata_csv",
    transformation_ctx="taxidata_node1659731970404",
)

# Script generated for node analyticscleandata
analyticscleandata_node1659731879890 = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name="gaanalytics_358212_analytics_322371309_events_intraday_",
    transformation_ctx="analyticscleandata_node1659731879890",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1659789442149 = glueContext.create_dynamic_frame.from_catalog(
    database=database,
    table_name="contact_csv",
    transformation_ctx="AWSGlueDataCatalog_node1659789442149",
)

# Script generated for node Join
Join_node1659731907396 = Join.apply(
    frame1=analyticscleandata_node1659731879890,
    frame2=taxidata_node1659731970404,
    keys1=["user_id"],
    keys2=["userid"],
    transformation_ctx="Join_node1659731907396",
)

# Script generated for node Select Fields
SelectFields_node1659734439401 = SelectFields.apply(
    frame=Join_node1659731907396,
    paths=[
        "event_name",
        "user_id",
        "user_pseudo_id",
        "user_first_touch_timestamp",
        "event_date",
        "fare_amount",
        "trip_distance",
        "passenger_count",
        "tip_amount",
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "geo.metro",
        "geo.city",
        "geo.country",
        "device.mobile_brand_name",
        "device.category",
    ],
    transformation_ctx="SelectFields_node1659734439401",
)

# Script generated for node Join-Contacts
JoinContacts_node1659789499340 = Join.apply(
    frame1=AWSGlueDataCatalog_node1659789442149,
    frame2=SelectFields_node1659734439401,
    keys1=["id"],
    keys2=["user_id"],
    transformation_ctx="JoinContacts_node1659789499340",
)


job.commit()
path = f"s3://{databucket}/analytics_taxi_riders"
# Script generated for node Amazon S3
AmazonS3_node1659720790855 = glueContext.getSink(
    path=path,
    connection_type="s3",
    partitionKeys=["event_name"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1659720790855",
)
AmazonS3_node1659720790855.setCatalogInfo(
    catalogDatabase=database, catalogTableName=table
)
AmazonS3_node1659720790855.setFormat("glueparquet")
AmazonS3_node1659720790855.writeFrame(JoinContacts_node1659789499340)
job.commit()

