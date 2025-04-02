WITH "_t2" AS (
  SELECT
    COUNT() AS "agg_0",
    "sbtransaction"."sbtxstatus" AS "status"
  FROM "main"."sbtransaction" AS "sbtransaction"
  GROUP BY
    "sbtransaction"."sbtxstatus"
), "_t0" AS (
  SELECT
    COALESCE("_t2"."agg_0", 0) AS "num_transactions",
    COALESCE("_t2"."agg_0", 0) AS "ordering_1",
    "_t2"."status" AS "status"
  FROM "_t2" AS "_t2"
  ORDER BY
    "ordering_1" DESC
  LIMIT 3
)
SELECT
  "_t0"."status" AS "status",
  "_t0"."num_transactions" AS "num_transactions"
FROM "_t0" AS "_t0"
ORDER BY
  "_t0"."ordering_1" DESC
