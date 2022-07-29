import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME','table','parentProject','connectionName','databucket'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
table=str(args['table'])
databucket= str(args['databucket'])


# Script generated for node Google BigQuery Connector 0.24.2 for AWS Glue 3.0
GoogleBigQueryConnector0242forAWSGlue30_node1 = (
    glueContext.create_dynamic_frame.from_options(
        connection_type="marketplace.spark",
        connection_options={
            "table": args["table"],
            "parentProject": args["parentProject"],
            "connectionName": args["connectionName"],
        },
        transformation_ctx="GoogleBigQueryConnector0242forAWSGlue30_node1",
    )
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=GoogleBigQueryConnector0242forAWSGlue30_node1,
    mappings=[
        ("date", "string", "date", "string"),
        ("channel_id", "string", "channel_id", "string"),
        ("video_id", "string", "video_id", "string"),
        ("live_or_on_demand", "string", "live_or_on_demand", "string"),
        ("subscribed_status", "string", "subscribed_status", "string"),
        ("country_code", "string", "country_code", "string"),
        ("traffic_source_type", "bigint", "traffic_source_type", "long"),
        ("traffic_source_detail", "string", "traffic_source_detail", "string"),
        ("views", "bigint", "views", "long"),
        ("watch_time_minutes", "double", "watch_time_minutes", "double"),
        (
            "average_view_duration_seconds",
            "double",
            "average_view_duration_seconds",
            "double",
        ),
        (
            "average_view_duration_percentage",
            "double",
            "average_view_duration_percentage",
            "double",
        ),
        ("red_views", "bigint", "red_views", "long"),
        ("red_watch_time_minutes", "double", "red_watch_time_minutes", "double"),
        ("_partitiontime", "timestamp", "_partitiontime", "timestamp"),
        ("_partitiondate", "date", "_partitiondate", "date"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path=f"s3://{databucket}/{table}/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="youtube", catalogTableName=table
)
S3bucket_node3.setFormat("glueparquet")
S3bucket_node3.writeFrame(ApplyMapping_node2)
job.commit()
