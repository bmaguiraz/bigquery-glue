# bigquery-glue


## YouTube Data

'''
{
    "data_bucket_name": "bigquery-youtube",
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
    "jobname": "bigquery-analytics",
    "data_bucket_name": "bmaguir-connector-data-bigquery",
    "table": "tfc-test-356921.youtube.events_*",
    "parentProject": "tfc-test-356921",
    "connectionName": "bigquery",
    "filter": "LIMIT 1000",
    "jobscript": "job-ga.py",
    "schedule": "0/10 * * * ? *",
    "gluedatabase": "youtube"
}
'''


## Queries


'''
SELECT device.category, count(*) as events 
FROM `tfc-test-356921.youtube.events_*`
GROUP BY 1
'''

'''
SELECT DISTINCT user_pseudo_id,
value.string_value

FROM `tfc-test-356921.youtube.events_*`,
UNNEST(user_properties) as up
where 
up.key is null

'''

### Count of page views 
'''
SELECT
value.string_value as page_name,count(*) as event_count
FROM `tfc-test-356921.youtube.events_*`,
UNNEST(event_params) as ep
where 
ep.key ="page_title" AND event_name = "page_view"
GROUP BY 1
ORDER BY 2 DESC
'''

### Count of users, new users, and sessions
'''
SELECT count(DISTINCT user_pseudo_id) as users,
countif(event_name="first_visit") as new_users,
countif(event_name="session_start") as sessions,
countif(event_name="fpage_view") as pageviews,
FROM `tfc-test-356921.youtube.events_*`


### User Funnel - Path to Conversion
'''

WITH base AS (
 SELECT 
    user_pseudo_id,
    event_timestamp,
    (SELECT value.int_value FROM unnest(event_params) WHERE key= "ga_session_id") as sessId,
    (SELECT value.string_value FROM unnest(event_params) WHERE key= "page_location") as pagePath,
 FROM `tfc-test-356921.youtube.events_*` WHERE event_name = "page_view"),
previousPagePathQ as (
  SELECT *,LAG(pagePath,1) OVER (PARTITION BY user_pseudo_id,sessId ORDER BY event_timestamp) as
  previousPagePath from base
)
SELECT pagePath,previousPagePath, count(DISTINCT CONCAT(user_pseudo_id,sessId)) as conversions,
count(*) as events
FROM previousPagePathQ WHERE pagePath like "%shop.%" GROUP BY 1,2 ORDER BY 3 DESC


'''

### Average Time between users first and N-th visit.  

Using attributed that describe first interactions (traffic source, timestamp)


'''

SELECT 
user_pseudo_id,sess_number, avg(daysSinceFirstInteraction), count(*) FROM (
SELECT 
user_pseudo_id,
(SELECT value.int_value FROM unnest (event_params) WHERE
key="a_session_number") sess_number,
MIN((event_timestamp -
user_first_touch_timestamp)/(1000000*60*60*24))
daysSinceFirstInteraction
FROM `tfc-test-356921.youtube.events_*`
GROUP BY 1,2
)
GROUP BY 1,2
ORDER BY 1

'''


### Purchasers
''' 
/**
 * Computes the audience of purchasers.
 *
 * Purchasers = users who have logged either in_app_purchase or
 * purchase.
 */
 
SELECT
  COUNT(DISTINCT user_id) AS purchasers_count
FROM
  -- PLEASE REPLACE WITH YOUR TABLE NAME.
  `tfc-test-356921.youtube.events_*`
WHERE
  event_name IN ('in_app_purchase', 'purchase')
  -- PLEASE REPLACE WITH YOUR DESIRED DATE RANGE
  AND _TABLE_SUFFIX BETWEEN '20180501' AND '20240131';
  
  
### N-day active users

''' 
/**
 * Builds an audience of N-Day Active Users.
 *
 * N-day active users = users who have logged at least one event with event param 
 * engagement_time_msec > 0 in the last N days.
*/

SELECT
  COUNT(DISTINCT user_id) AS n_day_active_users_count
FROM
  -- PLEASE REPLACE WITH YOUR TABLE NAME.
  `tfc-test-356921.youtube.events_*` AS T
    CROSS JOIN
      T.event_params
WHERE
  event_params.key = 'engagement_time_msec' AND event_params.value.int_value > 0
  -- Pick events in the last N = 20 days.
  AND event_timestamp >
      UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP, INTERVAL 20 DAY))
  -- PLEASE REPLACE WITH YOUR DESIRED DATE RANGE.
  AND _TABLE_SUFFIX BETWEEN '20180521' AND '20240131';
''' 

  
### N-day inactive users

'''
/**
 * Builds an audience of N-Day Inactive Users.
 *
 * N-Day inactive users = users in the last M days who have not logged one  
 * event with event param engagement_time_msec > 0 in the last N days 
 *  where M > N.
 */

 
SELECT
  COUNT(DISTINCT MDaysUsers.user_id) AS n_day_inactive_users_count
FROM
  (
    SELECT
      user_id
    FROM
      /* PLEASE REPLACE WITH YOUR TABLE NAME */
      `tfc-test-356921.youtube.events_*` AS T
    CROSS JOIN
      T.event_params
    WHERE
      event_params.key = 'engagement_time_msec' AND event_params.value.int_value > 0
      /* Has engaged in last M = 7 days */
      AND event_timestamp >
          UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
      /* PLEASE REPLACE WITH YOUR DESIRED DATE RANGE */
      AND _TABLE_SUFFIX BETWEEN '20180521' AND '20240131'
  ) AS MDaysUsers
-- EXCEPT ALL is not yet implemented in BigQuery. Use LEFT JOIN in the interim.
LEFT JOIN
  (
    SELECT
      user_id
    FROM
      /* PLEASE REPLACE WITH YOUR TABLE NAME */
      `tfc-test-356921.youtube.events_*` AS T
    CROSS JOIN
      T.event_params
    WHERE
      event_params.key = 'engagement_time_msec' AND event_params.value.int_value > 0
      /* Has engaged in last N = 2 days */
      AND event_timestamp >
          UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY))
      /* PLEASE REPLACE WITH YOUR DESIRED DATE RANGE */
      AND _TABLE_SUFFIX BETWEEN '20180521' AND '20240131'
  ) AS NDaysUsers
  ON MDaysUsers.user_id = NDaysUsers.user_id
WHERE
  NDaysUsers.user_id IS NULL;
''' 

 
### Frequently active users
''' 
/**
 * Builds an audience of Frequently Active Users.
 *
 * Frequently Active Users = users who have logged at least one
 * event with event param engagement_time_msec > 0 on N of 
 * the last M days where M > N.
 */

 
SELECT
  COUNT(DISTINCT user_id) AS frequent_active_users_count
FROM
  (
    SELECT
      user_id,
      COUNT(DISTINCT event_date)
    FROM
      -- PLEASE REPLACE WITH YOUR TABLE NAME.
      `tfc-test-356921.youtube.events_*` AS T
    CROSS JOIN
      T.event_params
    WHERE
      event_params.key = 'engagement_time_msec' AND event_params.value.int_value > 0
      -- User engagement in the last M = 10 days.
      AND event_timestamp >
          UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY))
      -- PLEASE REPLACE YOUR DESIRED DATE RANGE.  For optimal performance
      -- the _TABLE_SUFFIX range should match the INTERVAL value above.
      AND _TABLE_SUFFIX BETWEEN '20180521' AND '20240131'
    GROUP BY 1
    -- Having engaged in at least N = 4 days.
    HAVING COUNT(event_date) >= 4
  );
''' 

 
### Highly active users
''' 

/**
 * Builds an audience of Highly Active Users.
 *
 * Highly Active Users = users who have been active for more than N minutes
 * in the last M days where M > N.
*/

SELECT
  COUNT(DISTINCT user_id) AS high_active_users_count
FROM
  (
    SELECT
      user_id,
      event_params.key,
      SUM(event_params.value.int_value)
    FROM
      -- PLEASE REPLACE WITH YOUR TABLE NAME.
      `tfc-test-356921.youtube.events_*` AS T
    CROSS JOIN
      T.event_params
    WHERE
      -- User engagement in the last M = 10 days.
      event_timestamp >
          UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 DAY))
      AND event_params.key = 'engagement_time_msec'
      -- PLEASE REPLACE YOUR DESIRED DATE RANGE.
      AND _TABLE_SUFFIX BETWEEN '20180521' AND '20240131'
    GROUP BY 1, 2
    HAVING
      -- Having engaged for more than N = 0.1 minutes.
      SUM(event_params.value.int_value) > 0.1 * 60 * 1000000
  );
''' 



### Acquired users
''' 
/**
 * Builds an audience of Acquired Users.
 *
 * Acquired Users = users who were acquired via some Source/Medium/Campaign.
 */
 
SELECT
  COUNT(DISTINCT user_id) AS acquired_users_count
FROM
  -- PLEASE REPLACE WITH YOUR TABLE NAME.
  `tfc-test-356921.youtube.events_*`
WHERE
  traffic_source.source = 'google'
  AND traffic_source.medium = 'cpc'
  AND traffic_source.name = 'VTA-Test-Android'
  -- PLEASE REPLACE YOUR DESIRED DATE RANGE.
  AND _TABLE_SUFFIX BETWEEN '20180521' AND '20240131';
'''



### Cohorts with filters
''' 
/**
 * Builds an audience composed of users acquired last week
 * through Google campaigns, i.e., cohorts with filters.
 *
 * Cohort is defined as users acquired last week, i.e. between 7 - 14
 * days ago. The cohort filter is for users acquired through a direct
 * campaign.
 */
 
SELECT
  COUNT(DISTINCT user_id) AS users_acquired_through_google_count
FROM
  `tfc-test-356921.youtube.events_*`
WHERE
  event_name = 'first_open'
  AND event_timestamp >
      UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 14 DAY))
  AND event_timestamp <
      UNIX_MICROS(TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
  AND traffic_source.source = 'google'

  AND _TABLE_SUFFIX BETWEEN '20180501' AND '20240131';
  '''

### Average amount of money spent per purchase session by user
The following query shows the average amount of money spent per session by each user. This takes into account only the sessions where the user made a purchase.

https://developers.google.com/analytics/bigquery/advanced-queries#user_pseudo_id

'''

SELECT
  user_pseudo_id,
  COUNT(
    DISTINCT(SELECT EP.value.int_value FROM UNNEST(event_params) AS EP WHERE key = 'ga_session_id'))
    AS session_count,
  AVG(
    (
      SELECT COALESCE(EP.value.int_value, EP.value.float_value, EP.value.double_value)
      FROM UNNEST(event_params) AS EP
      WHERE key = 'value'
    )) AS avg_spend_per_session_by_user,
FROM
  -- Replace table name.
  `tfc-test-356921.youtube.events_*`
WHERE
  event_name = 'purchase'
  -- Replace date range.
  AND _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
GROUP BY
  1;

  '''

### Latest Session Id and Session Number for users

The following query provides the list of the latest ga_session_id and ga_session_number from last 4 days for a list of users. You can provide either a user_pseudo_id list or a user_id list.

-- Get the latest ga_session_id and ga_session_number for specific users during last 4 days.
-- Replace timezone. List at https://en.wikipedia.org/wiki/List_of_tz_database_time_zones.
-- Replace list of user_pseudo_id's with ones you want to query.

'''
DECLARE REPORTING_TIMEZONE STRING DEFAULT 'America/New_York';


DECLARE USER_PSEUDO_ID_LIST ARRAY<STRING> DEFAULT
  [
    '1005355938.1632145814', '979622592.1632496588', '1101478530.1632831095'];

CREATE TEMP FUNCTION GetParamValue(params ANY TYPE, target_key STRING)
AS (
  (SELECT `value` FROM UNNEST(params) WHERE key = target_key LIMIT 1)
);

CREATE TEMP FUNCTION GetDateSuffix(date_shift INT64, timezone STRING)
AS (
  (SELECT FORMAT_DATE('%Y%m%d', DATE_ADD(CURRENT_DATE(timezone), INTERVAL date_shift DAY)))
);

SELECT DISTINCT
  user_pseudo_id,
  FIRST_VALUE(GetParamValue(event_params, 'ga_session_id').int_value)
    OVER (UserWindow) AS ga_session_id,
  FIRST_VALUE(GetParamValue(event_params, 'ga_session_number').int_value)
    OVER (UserWindow) AS ga_session_number
FROM
  -- Replace table name.
  `tfc-test-356921.youtube.events_*`
WHERE
  user_pseudo_id IN UNNEST(USER_PSEUDO_ID_LIST)
  AND RIGHT(_TABLE_SUFFIX, 8)
    BETWEEN GetDateSuffix(-3, REPORTING_TIMEZONE)
    AND GetDateSuffix(0, REPORTING_TIMEZONE)
WINDOW UserWindow AS (PARTITION BY user_pseudo_id ORDER BY event_timestamp DESC);

'''

### Products purchased by customers who purchased a certain product
The following query shows what other products were purchased by customers who purchased a specific product. This example does not assume that the products were purchased in the same order.

'''
-- Example: Products purchased by customers who purchased a specific product.
--
-- `Params` is used to hold the value of the selected product and is referenced
-- throughout the query.

WITH
  Params AS (
    -- Replace with selected item_name or item_id.
    SELECT 'Google Navy Speckled Tee' AS selected_product
  ),
  PurchaseEvents AS (
    SELECT
      user_pseudo_id,
      items
    FROM
      -- Replace table name.
  `tfc-test-356921.youtube.events_*`
    WHERE
      -- Replace date range.
      _TABLE_SUFFIX BETWEEN '20201101' AND '20210131'
      AND event_name = 'purchase'
  ),
  ProductABuyers AS (
    SELECT DISTINCT
      user_pseudo_id
    FROM
      Params,
      PurchaseEvents,
      UNNEST(items) AS items
    WHERE
      -- item.item_id can be used instead of items.item_name.
      items.item_name = selected_product
  )
SELECT
  items.item_name AS item_name,
  SUM(items.quantity) AS item_quantity
FROM
  Params,
  PurchaseEvents,
  UNNEST(items) AS items
WHERE
  user_pseudo_id IN (SELECT user_pseudo_id FROM ProductABuyers)
  -- item.item_id can be used instead of items.item_name
  AND items.item_name != selected_product
GROUP BY 1
ORDER BY item_quantity DESC;

'''