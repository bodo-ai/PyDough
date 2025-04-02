WITH "_t0" AS (
  SELECT
    "notifications"."created_at" AS "created_at",
    "notifications"."user_id" AS "user_id"
  FROM "main"."notifications" AS "notifications"
), "_t1" AS (
  SELECT
    "users"."created_at" AS "created_at",
    "users"."uid" AS "uid"
  FROM "main"."users" AS "users"
), "_t0_2" AS (
  SELECT
    "_t0"."created_at" AS "created_at",
    "_t1"."created_at" AS "created_at_1",
    "_t0"."user_id" AS "user_id"
  FROM "_t0" AS "_t0"
  LEFT JOIN "_t1" AS "_t1"
    ON "_t0"."user_id" = "_t1"."uid"
  WHERE
    "_t0"."created_at" <= DATETIME("_t1"."created_at", '1 year')
    AND "_t0"."created_at" >= "_t1"."created_at"
), "_t3" AS (
  SELECT
    "_t0"."user_id" AS "user_id"
  FROM "_t0_2" AS "_t0"
), "_t2" AS (
  SELECT
    "users"."created_at" AS "created_at",
    "users"."email" AS "email",
    "users"."uid" AS "uid",
    "users"."username" AS "username"
  FROM "main"."users" AS "users"
)
SELECT
  "_t2"."username" AS "username",
  "_t2"."email" AS "email",
  "_t2"."created_at" AS "created_at"
FROM "_t2" AS "_t2"
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM "_t3" AS "_t3"
    WHERE
      "_t2"."uid" = "_t3"."user_id"
  )
