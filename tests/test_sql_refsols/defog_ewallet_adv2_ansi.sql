WITH "_t1" AS (
  SELECT
    COUNT() AS "agg_0",
    SUM((
      (
        DAY_OF_WEEK("notifications"."created_at") + 6
      ) % 7
    ) IN (5, 6)) AS "agg_1",
    "notifications"."user_id" AS "user_id",
    DATE_TRUNC('WEEK', CAST("notifications"."created_at" AS TIMESTAMP)) AS "week"
  FROM "main"."notifications" AS "notifications"
  WHERE
    "notifications"."created_at" < DATE_TRUNC('WEEK', CURRENT_TIMESTAMP())
    AND "notifications"."created_at" >= DATE_ADD(DATE_TRUNC('WEEK', CURRENT_TIMESTAMP()), -3, 'WEEK')
  GROUP BY
    "notifications"."user_id",
    DATE_TRUNC('WEEK', CAST("notifications"."created_at" AS TIMESTAMP))
), "_t0_2" AS (
  SELECT
    SUM("_t1"."agg_0") AS "agg_0",
    SUM("_t1"."agg_1") AS "agg_1",
    "_t1"."week" AS "week"
  FROM "main"."users" AS "users"
  JOIN "_t1" AS "_t1"
    ON "_t1"."user_id" = "users"."uid"
  WHERE
    "users"."country" IN ('US', 'CA')
  GROUP BY
    "_t1"."week"
)
SELECT
  "_t0"."week" AS "week",
  COALESCE("_t0"."agg_0", 0) AS "num_notifs",
  COALESCE("_t0"."agg_1", 0) AS "weekend_notifs"
FROM "_t0_2" AS "_t0"
