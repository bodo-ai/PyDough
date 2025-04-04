WITH "_t1" AS (
  SELECT
    COUNT() AS "agg_0",
    "user_sessions"."device_type" AS "device_type"
  FROM "main"."user_sessions" AS "user_sessions"
  GROUP BY
    "user_sessions"."device_type"
)
SELECT
  "_t1"."device_type" AS "device_type",
  COALESCE("_t1"."agg_0", 0) AS "count"
FROM "_t1" AS "_t1"
ORDER BY
  "count" DESC
LIMIT 2
