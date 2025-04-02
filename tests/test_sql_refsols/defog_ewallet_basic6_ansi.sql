WITH "_t2" AS (
  SELECT
    COUNT() AS "agg_0",
    "user_sessions"."device_type" AS "device_type"
  FROM "main"."user_sessions" AS "user_sessions"
  GROUP BY
    "user_sessions"."device_type"
), "_t0" AS (
  SELECT
    COALESCE("_t2"."agg_0", 0) AS "count",
    "_t2"."device_type" AS "device_type",
    COALESCE("_t2"."agg_0", 0) AS "ordering_1"
  FROM "_t2" AS "_t2"
  ORDER BY
    "ordering_1" DESC
  LIMIT 2
)
SELECT
  "_t0"."device_type" AS "device_type",
  "_t0"."count" AS "count"
FROM "_t0" AS "_t0"
ORDER BY
  "_t0"."ordering_1" DESC
