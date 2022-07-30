import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME','table','parentProject','connectionName','databucket','filter'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
table=str(args['table'])
databucket= str(args['databucket'])

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)



# Script generated for node Google BigQuery Connector 0.24.2 for AWS Glue 3.0
# connector additional options https://github.com/GoogleCloudDataproc/spark-bigquery-connector/tree/0.24.2
GoogleBigQueryConnector0242forAWSGlue30_node1 = (
    glueContext.create_dynamic_frame.from_options(
        connection_type="marketplace.spark",
        connection_options={
            "table": args["table"],
            "parentProject": args["parentProject"],
            "connectionName": args["connectionName"],
            #"filter": args["filter"],
        },
        transformation_ctx="GoogleBigQueryConnector0242forAWSGlue30_node1",
    )
)


# Script generated for node SQL
SqlQuery0 = """
select *,sha2(country_code,256) as country_code_hashed from youtubedata

"""
SQL_node1659192237456 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"youtubedata": GoogleBigQueryConnector0242forAWSGlue30_node1},
    transformation_ctx="SQL_node1659192237456",
)

# Script generated for node Select Fields
SelectFields_node1659197478137 = SelectFields.apply(
    frame=SQL_node1659192237456,
    paths=[
        "country_code_hashed",
        "date",
        "channel_id",
        "video_id",
        "live_or_on_demand",
        "subscribed_status",
        "traffic_source_type",
        "traffic_source_detail",
        "views",
        "watch_time_minutes",
        "average_view_duration_seconds",
        "average_view_duration_percentage",
        "red_views",
        "red_watch_time_minutes",
        "_partitiontime",
        "_partitiondate",
    ],
    transformation_ctx="SelectFields_node1659197478137",
)


# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path=f"s3://{databucket}/{table}/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=["date"],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="youtube", catalogTableName=table
)
S3bucket_node3.setFormat("glueparquet")
S3bucket_node3.writeFrame(SelectFields_node1659197478137)
job.commit()
