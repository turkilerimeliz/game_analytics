-- Day0 Impressions for Interstitital and Rewarded 
/*
-- Run when you first create the table
CREATE OR REPLACE TABLE `spot-the-differences-57838.Spot_Diff_Funnel.Ads`
OPTIONS(
  description="Interstitials and Rewarded info"
) AS
*/

-- Delete last five days included in the current table
DELETE `dataset_id.Ads`
WHERE cohort BETWEEN '2022-12-24' AND '2023-01-02';

-- Expand the cohort interval (include latest release)
INSERT `dataset_id.Ads` (
  country,
  app_Version,
  cohort,
  unique_user,
  interstitial_shown,
  rewarded_shown
)
SELECT
  m.country,
  m.app_version,
  CAST(m.firstOpen_date AS DATE FORMAT 'YYYYMMDD') AS cohort,
  COUNT(DISTINCT m.userid) AS unique_user,
  SUM(n.intersitital) AS interstitial_shown,
  SUM(n.rewarded) AS rewarded_shown
FROM (
  SELECT
    DISTINCT(user_pseudo_id) AS userid,
    event_date AS firstOpen_date,
    TIMESTAMP_MICROS(event_timestamp) AS firstOpen_time,
    geo.country AS country,
    app_info.version AS app_version
  FROM
    `dataset_id.events_*`,
    UNNEST(event_params) AS p
  WHERE
    event_name = 'first_open'
    AND p.key = 'firebase_conversion'
    AND p.value.int_value = 1
    AND _TABLE_SUFFIX BETWEEN '20221224' AND '20230104'
) AS m
LEFT JOIN (
  SELECT
    DISTINCT(userid) AS userid,
    t.country,
    t.app_Version,
    CAST(t.firstOpen_date AS DATE FORMAT 'YYYYMMDD') AS open_date,
    l.event_name,
    COUNT(value),
    CASE
      WHEN l.event_name = 'interstitial_shown' THEN COUNT(value)
      ELSE 0
    END AS intersitital,
    CASE
      WHEN l.event_name = 'rewarded_shown' THEN COUNT(value)
      ELSE 0
    END AS rewarded
  FROM (
    SELECT
      DISTINCT(user_pseudo_id) AS userid,
      event_date AS firstOpen_date,
      TIMESTAMP_MICROS(event_timestamp) AS firstOpen_time,
      geo.country AS country,
      app_info.version AS app_Version
    FROM
      `dataset_id.events_*`,
      UNNEST(event_params) AS p
    WHERE
      event_name = 'first_open'
      AND p.key = 'firebase_conversion'
      AND p.value.int_value = 1
      AND _TABLE_SUFFIX BETWEEN '20221224' AND '20230104'
  ) AS t
  LEFT JOIN (
    SELECT
      user_pseudo_id,
      event_date,
      TIMESTAMP_MICROS(event_timestamp) AS event_timestamp,
      event_name,
      p.key AS key,
      p.value.int_value AS value
    FROM
      `dataset_id.events_*`,
      UNNEST(event_params) AS p
    WHERE
      (event_name = 'interstitial_shown' OR event_name = 'rewarded_shown')
      AND p.key = 'engaged_session_event'
      AND p.value.int_value = 1
      AND _TABLE_SUFFIX BETWEEN '20221224' AND '20230104'
  ) AS l
  ON t.userid = l.user_pseudo_id
  WHERE TIMESTAMP_DIFF(l.event_timestamp, t.firstOpen_time, HOUR) <= 24
  GROUP BY 1, 2, 3, 4, 5
  ORDER BY 1, 2, 3, 4, 5
) AS n
ON m.userid = n.userid
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;

