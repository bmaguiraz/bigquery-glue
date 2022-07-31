# bigquery-glue


## YouTube Data

'''
{
    "data_bucket_name": "bmaguir-connector-data-bigquery",
    "table": "tfc-test-356921.youtube.p_channel_traffic_source_a2_youtube",
    "parentProject": "tfc-test-356921",
    "connectionName": "bigquery",
    "filter": "DATE(_PARTITIONTIME) <= CURRENT_DATE()",
    "jobscript": "job.py",
    "schedule": "0/10 * * * ? *",
    "gluedatabase": "youtube"
}
'''

## Google Analytics 4 Data

'''
{
    "data_bucket_name": "bmaguir-connector-data-bigquery",
    "table": "tfc-test-356921.youtube.events_20210131",
    "parentProject": "tfc-test-356921",
    "connectionName": "bigquery",
    "filter": "LIMIT 1000",
    "jobscript": "job-ga.py",
    "schedule": "0/10 * * * ? *",
    "gluedatabase": "youtube"
}
'''


