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


args = getResolvedOptions(sys.argv, ['JOB_NAME','database','table','parentProject','connectionName','databucket','filter'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

table=str(args['table'])
table="analytics_taxi_riders"
databucket= str(args['databucket'])
database=str(args['database'])

# Script generated for node taxidata
taxidata_node1659731970404 = glueContext.create_dynamic_frame.from_catalog(
    database="youtube",
    table_name="taxitrip_data",
    transformation_ctx="taxidata_node1659731970404",
)

# Script generated for node analytics
analytics_node1659731879890 = glueContext.create_dynamic_frame.from_catalog(
    database="youtube",
    table_name="analytics_events_daily",
    transformation_ctx="analytics_node1659731879890",
)

# Script generated for node Join
Join_node1659731907396 = Join.apply(
    frame1=analytics_node1659731879890,
    frame2=taxidata_node1659731970404,
    keys1=["user_id"],
    keys2=["userid"],
    transformation_ctx="Join_node1659731907396",
)

# Script generated for node taxi-analytics-join
taxianalyticsjoin_node1659734439401 = SelectFields.apply(
    frame=Join_node1659731907396,
    paths=[
        "event_name",
        "user_id",
        "user_pseudo_id",
        "user_first_touch_timestamp",
        "fare_amount",
        "lpep_pickup_datetime",
        "event_date",
        "passenger_count",
    ],
    transformation_ctx="taxianalyticsjoin_node1659734439401",
)

path = f"s3://{databucket}/{table}"
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
AmazonS3_node1659720790855.writeFrame(taxianalyticsjoin_node1659734439401)
job.commit()

