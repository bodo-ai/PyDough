WITH "_t1" AS (
  SELECT
    COUNT() AS "agg_0",
    SUM(
      (
        (
          CAST(STRFTIME('%w', "notifications"."created_at") AS INTEGER) + 6
        ) % 7
      ) IN (5, 6)
    ) AS "agg_1",
    "notifications"."user_id" AS "user_id",
    DATE(
      "notifications"."created_at",
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME("notifications"."created_at")) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    ) AS "week"
  FROM "main"."notifications" AS "notifications"
  WHERE
    "notifications"."created_at" < DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
    AND "notifications"."created_at" >= DATE(
      'now',
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME('now')) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day',
      '-21 day'
    )
  GROUP BY
    "notifications"."user_id",
    DATE(
      "notifications"."created_at",
      '-' || CAST((
        CAST(STRFTIME('%w', DATETIME("notifications"."created_at")) AS INTEGER) + 6
      ) % 7 AS TEXT) || ' days',
      'start of day'
    )
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
