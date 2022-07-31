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


args = getResolvedOptions(sys.argv, ['JOB_NAME','table','parentProject','connectionName','databucket','filter'])
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
            "viewsEnabled": "true",
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
select *,sha2(platform,256) as user_id_hash from analytics

"""
SQL_node1659274851804 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"analytics": GoogleBigQueryConnector0242forAWSGlue30_node1},
    transformation_ctx="SQL_node1659274851804",
)

# Script generated for node Select Fields
SelectFields_node1659274873316 = SelectFields.apply(
    frame=SQL_node1659274851804,
    paths=[
        "user_id_hash",
        "event_date",
        "event_timestamp",
        "event_name",
        "event_params.key",
        "event_params.value.string_value",
        "event_params.value.int_value",
        "event_params.value.float_value",
        "event_params.value.double_value",
        "event_previous_timestamp",
        "event_value_in_usd",
        "event_bundle_sequence_id",
        "event_server_timestamp_offset",
        "user_pseudo_id",
        "privacy_info.analytics_storage",
        "privacy_info.ads_storage",
        "privacy_info.uses_transient_token",
        "user_properties.key",
        "user_properties.value.string_value",
        "user_properties.value.int_value",
        "user_properties.value.float_value",
        "user_properties.value.double_value",
        "user_properties.value.set_timestamp_micros",
        "user_first_touch_timestamp",
        "user_ltv.revenue",
        "user_ltv.currency",
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
        "device.web_info.browser",
        "device.web_info.browser_version",
        "geo.continent",
        "geo.sub_continent",
        "geo.country",
        "geo.region",
        "geo.city",
        "geo.metro",
        "app_info.id",
        "app_info.version",
        "app_info.install_store",
        "app_info.firebase_app_id",
        "app_info.install_source",
        "traffic_source.medium",
        "traffic_source.name",
        "traffic_source.source",
        "stream_id",
        "platform",
        "event_dimensions.hostname",
        "ecommerce.total_item_quantity",
        "ecommerce.purchase_revenue_in_usd",
        "ecommerce.purchase_revenue",
        "ecommerce.refund_value_in_usd",
        "ecommerce.refund_value",
        "ecommerce.shipping_value_in_usd",
        "ecommerce.shipping_value",
        "ecommerce.tax_value_in_usd",
        "ecommerce.tax_value",
        "ecommerce.unique_items",
        "ecommerce.transaction_id",
        "items.item_id",
        "items.item_name",
        "items.item_brand",
        "items.item_variant",
        "items.item_category",
        "items.item_category2",
        "items.item_category3",
        "items.item_category4",
        "items.item_category5",
        "items.price_in_usd",
        "items.price",
        "items.quantity",
        "items.item_revenue_in_usd",
        "items.item_revenue",
        "items.item_refund_in_usd",
        "items.item_refund",
        "items.coupon",
        "items.affiliation",
        "items.location_id",
        "items.item_list_id",
        "items.item_list_name",
        "items.item_list_index",
        "items.promotion_id",
        "items.promotion_name",
        "items.creative_name",
        "items.creative_slot",
    ],
    transformation_ctx="SelectFields_node1659274873316",
)

path = f"s3://{databucket}/{table}"

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=SelectFields_node1659274873316,
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
