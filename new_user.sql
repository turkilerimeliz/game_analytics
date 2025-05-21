-- Number of Daily New User by Country and App Version
SELECT
  geo.country AS country,
  app_info.version AS app_version,
  COUNT(DISTINCT user_pseudo_id) AS number_user
FROM
  `dataset_id.events_*`,
  UNNEST(event_params) AS p
WHERE
  event_name = 'first_open'
  AND p.key = 'firebase_conversion'
  AND p.value.int_value = 1
  AND _TABLE_SUFFIX BETWEEN '20221024' AND '20221030'
GROUP BY
  1, 2;
