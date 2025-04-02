WITH "_t1" AS (
  SELECT
    SUM(
      DATEDIFF("user_sessions"."session_end_ts", "user_sessions"."session_start_ts", SECOND)
    ) AS "agg_0",
    "user_sessions"."user_id" AS "user_id"
  FROM "main"."user_sessions" AS "user_sessions"
  WHERE
    "user_sessions"."session_end_ts" < '2023-06-08'
    AND "user_sessions"."session_start_ts" >= '2023-06-01'
  GROUP BY
    "user_sessions"."user_id"
)
SELECT
  "users"."uid" AS "uid",
  COALESCE("_t1"."agg_0", 0) AS "total_duration"
FROM "main"."users" AS "users"
JOIN "_t1" AS "_t1"
  ON "_t1"."user_id" = "users"."uid"
ORDER BY
  COALESCE("_t1"."agg_0", 0) DESC
