WITH "_t1" AS (
  SELECT
    COUNT() AS "agg_0",
    "sbtransaction"."sbtxstatus" AS "status"
  FROM "main"."sbtransaction" AS "sbtransaction"
  GROUP BY
    "sbtransaction"."sbtxstatus"
)
SELECT
  "_t1"."status" AS "status",
  COALESCE("_t1"."agg_0", 0) AS "num_transactions"
FROM "_t1" AS "_t1"
ORDER BY
  "num_transactions" DESC
LIMIT 3
