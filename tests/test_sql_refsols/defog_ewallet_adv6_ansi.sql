WITH "_t0" AS (
  SELECT
    "users"."uid" AS "uid"
  FROM "main"."users" AS "users"
), "_t1" AS (
  SELECT
    "wallet_user_balance_daily"."user_id" AS "user_id"
  FROM "main"."wallet_user_balance_daily" AS "wallet_user_balance_daily"
), "_t6" AS (
  SELECT
    "_t0"."uid" AS "uid"
  FROM "_t0" AS "_t0"
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM "_t1" AS "_t1"
      WHERE
        "_t0"."uid" = "_t1"."user_id"
    )
), "_t4" AS (
  SELECT
    "_t2"."uid" AS "uid"
  FROM "_t0" AS "_t2"
  WHERE
    EXISTS(
      SELECT
        1 AS "1"
      FROM "_t1" AS "_t3"
      WHERE
        "_t2"."uid" = "_t3"."user_id"
    )
), "_t5" AS (
  SELECT
    "wallet_user_balance_daily"."balance" AS "balance",
    "wallet_user_balance_daily"."updated_at" AS "updated_at",
    "wallet_user_balance_daily"."user_id" AS "user_id"
  FROM "main"."wallet_user_balance_daily" AS "wallet_user_balance_daily"
), "_t1_2" AS (
  SELECT
    "_t5"."balance" AS "balance",
    "_t4"."uid" AS "uid",
    "_t5"."updated_at" AS "updated_at"
  FROM "_t4" AS "_t4"
  JOIN "_t5" AS "_t5"
    ON "_t4"."uid" = "_t5"."user_id"
), "_t0_2" AS (
  SELECT
    "_t1"."balance" AS "balance",
    "_t1"."uid" AS "uid"
  FROM "_t1_2" AS "_t1"
  QUALIFY
    ROW_NUMBER() OVER (PARTITION BY "_t1"."uid" ORDER BY "_t1"."updated_at" DESC NULLS FIRST) = 1
), "_t7" AS (
  SELECT
    "_t0"."balance" AS "balance",
    "_t0"."uid" AS "uid"
  FROM "_t0_2" AS "_t0"
)
SELECT
  "_t6"."uid" AS "user_id",
  "_t7"."balance" AS "latest_balance"
FROM "_t6" AS "_t6"
LEFT JOIN "_t7" AS "_t7"
  ON "_t6"."uid" = "_t7"."uid"
