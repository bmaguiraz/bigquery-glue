import sys
import pytz
from datetime import datetime
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


args = getResolvedOptions(sys.argv, ['JOB_NAME','table','parentProject','connectionName','databucket','filter'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

table=str(args['table'])
databucket= str(args['databucket'])
now = datetime.now(pytz.timezone('US/Eastern'))
format = "%Y%m%d"

table_suffix = now.strftime(format)
table_full = table + table_suffix


# Script generated for node analyticsdata
analyticsdata_node1659719961623 = glueContext.create_dynamic_frame.from_options(
    connection_type="marketplace.spark",
    connection_options={
            "viewsEnabled": "true",
            "table": table_full.strip(),
            "parentProject": args["parentProject"],
            "connectionName": args["connectionName"],
            "filter": args["filter"],
    },
    transformation_ctx="analyticsdata_node1659719961623",
)

# Script generated for node analyticsfields
analyticsfields_node1659719907216 = SelectFields.apply(
    frame=analyticsdata_node1659719961623,
    paths=[
        "user_id",
        "event_name",
        "device.category",
        "device.mobile_brand_name",
        "device.mobile_model_name",
        "device.mobile_marketing_name",
        "device.mobile_os_hardware_model",
        "device.operating_system",
        "device.operating_system_version",
        "device.vendor_id",
        "device.advertising_id",
        "device.language",
        "device.is_limited_ad_tracking",
        "device.time_zone_offset_seconds",
        "device.browser",
        "device.browser_version",
        "event_date",
        "user_pseudo_id",
        "traffic_source.name",
        "traffic_source.medium",
        "traffic_source.source",
        "geo.continent",
        "geo.country",
        "geo.region",
        "geo.city",
        "geo.sub_continent",
        "geo.metro",
        "user_first_touch_timestamp",
    ],
    transformation_ctx="analyticsfields_node1659719907216",
)

# Script generated for node select-join
selectjoin_node1659721205082 = SelectFields.apply(
    frame=analyticsfields_node1659719907216,
    paths=[
        "user_id",
        "event_name",
        "device.category",
        "device.mobile_brand_name",
        "device.mobile_model_name",
        "device.mobile_marketing_name",
        "device.mobile_os_hardware_model",
        "device.operating_system",
        "device.operating_system_version",
        "device.vendor_id",
        "device.advertising_id",
        "device.language",
        "device.is_limited_ad_tracking",
        "device.time_zone_offset_seconds",
        "device.browser",
        "device.browser_version",
        "event_date",
        "user_pseudo_id",
        "traffic_source.name",
        "traffic_source.medium",
        "traffic_source.source",
        "geo.continent",
        "geo.country",
        "geo.region",
        "geo.city",
        "geo.sub_continent",
        "geo.metro",
        "user_first_touch_timestamp"
    ],
    transformation_ctx="selectjoin_node1659721205082",
)

path = f"s3://{databucket}/{table}"

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=selectjoin_node1659721205082,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": path,
        "partitionKeys": ["event_date"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="S3bucket_node3",
)

job.commit()
