 CREATE OR REPLACE TABLE `dataset_id.Level`
OPTIONS(
  description="Detailed level progress info"
) AS
SELECT
  Userid,
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
    AND _TABLE_SUFFIX BETWEEN '20221119' AND '20230104'
) AS t
LEFT JOIN (
  SELECT
    user_pseudo_id,
    Event_date,
    Event_timestamp,
    Event_name,
    p.key AS Key,
    p.value.string_value AS Value
  FROM
    `dataset_id.events_*`,
    UNNEST(event_params) AS p
  WHERE
    event_name = 'LevelProgress'
    AND p.key = 'levelCompleted'
    AND _TABLE_SUFFIX BETWEEN '20221119' AND '20230104'
) AS l
ON
  t.Userid = l.user_pseudo_id
ORDER BY
  Userid,
  Value;

INSERT `dataset_id.Level` (
  Userid,
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
  DISTINCT(Userid) AS Userid,
  open_date,
  FirstOpen_time,
  "Same as Open Date" AS Event_date,
  FirstOpen_time AS Event_timestamp,
  "First Open" AS Event_name,
  "First Open" AS Key,
  "Level_0" AS Value,
  Country,
  App_Version
FROM
  `dataset_id.Level`;

