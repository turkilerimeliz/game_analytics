-- A/B Test Results: Churn Rate Analysis Across Levels, Countries, and App Versions

/*
CREATE OR REPLACE TABLE `dataset_id.AB`
OPTIONS(
  description="AB Test results"
) AS
*/

-- Delete previous experiment data
DELETE `dataset_id.AB`
WHERE Experiment_name = "LevelOrderABTest3";

-- Insert AB test results from LevelProgress events
INSERT `dataset_id.AB` (
  Userid,
  Experiment_name,
  Experiment_variant,
  open_date,
  FirstOpen_time,
  Event_date,
  Event_timestamp,
  Event_name,
  Key,
  Value,
  Country,
  App_Version
)
SELECT
  Userid,
  l.experimentName AS Experiment_name,
  l.experimentVariant AS Experiment_variant,
  CAST(t.FirstOpen_date AS DATE FORMAT 'YYYYMMDD') AS open_date,
  t.FirstOpen_time,
  Event_date,
  Event_timestamp,
  Event_name,
  Key,
  Value,
  t.Country,
  t.App_Version
FROM (
  SELECT
    DISTINCT(user_pseudo_id) AS Userid,
    event_date AS FirstOpen_date,
    event_timestamp AS FirstOpen_time,
    geo.country AS Country,
    app_info.version AS App_Version
  FROM
    `dataset_id.events_*`,
    UNNEST(event_params) AS p
  WHERE
    event_name = 'first_open'
    AND p.key = 'firebase_conversion'
    AND p.value.int_value = 1
    AND _TABLE_SUFFIX BETWEEN '20230102' AND '20230104'
) AS t
INNER JOIN (
  SELECT
    "LevelOrderABTest3" AS experimentName,
    user_pseudo_id,
    Event_date,
    Event_timestamp,
    Event_name,
    CASE userProperty.value.string_value
      WHEN "0" THEN "Baseline"
      WHEN "1" THEN "Variant A"
    END AS experimentVariant,
    p.key AS Key,
    p.value.string_value AS Value
  FROM
    `dataset_id.events_*`,
    UNNEST(event_params) AS p,
    UNNEST(user_properties) AS userProperty
  WHERE
    event_name = 'LevelProgress'
    AND p.key = 'levelCompleted'
    AND userProperty.key = "firebase_exp_4"
    AND _TABLE_SUFFIX BETWEEN '20230102' AND '20230104'
) AS l
ON t.Userid = l.user_pseudo_id
WHERE l.experimentName IS NOT NULL
ORDER BY
  Userid,
  Value;

-- Insert synthetic "First Open" rows as Level_0 for AB variants
INSERT `dataset_id.AB` (
  Userid,
  Experiment_name,
  Experiment_variant,
  open_date,
  FirstOpen_time,
  Event_date,
  Event_timestamp,
  Event_name,
  Key,
  Value,
  Country,
  App_Version
)
SELECT
  DISTINCT(t.Userid) AS Userid,
  Experiment_name,
  Experiment_variant,
  CAST(event_date AS DATE FORMAT 'YYYYMMDD') AS open_date,
  event_timestamp AS FirstOpen_time,
  event_date AS Event_date,
  event_timestamp AS Event_timestamp,
  "First Open" AS Event_name,
  "First Open" AS Key,
  "Level_0" AS Value,
  Country,
  App_Version
FROM (
  SELECT
    user_pseudo_id AS Userid,
    CAST(event_date AS DATE FORMAT 'YYYYMMDD') AS open_date,
    event_timestamp AS FirstOpen_time,
    event_date AS Event_date,
    event_timestamp AS Event_timestamp,
    geo.country AS Country,
    app_info.version AS App_Version
  FROM
    `dataset_id.events_*`,
    UNNEST(event_params) AS p
  WHERE
    event_name = 'first_open'
    AND p.key = 'firebase_conversion'
    AND p.value.int_value = 1
    AND _TABLE_SUFFIX BETWEEN '20230102' AND '20230104'
) AS t
INNER JOIN (
  SELECT
    DISTINCT(user_pseudo_id) AS Userid,
    "LevelOrderABTest3" AS Experiment_name,
    CASE userProperty.value.string_value
      WHEN "0" THEN "Baseline"
      WHEN "1" THEN "Variant A"
    END AS Experiment_variant
  FROM
    `dataset_id.events_*`,
    UNNEST(user_properties) AS userProperty
  WHERE
    _TABLE_SUFFIX BETWEEN '20230102' AND '20230104'
    AND userProperty.key = "firebase_exp_4"
) AS ab
ON t.Userid = ab.Userid;
