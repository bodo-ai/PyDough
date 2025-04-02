WITH "_t0" AS (
  SELECT
    AVG("sbtransaction"."sbtxprice") AS "avg_price",
    DATE_TRUNC('MONTH', CAST("sbtransaction"."sbtxdatetime" AS TIMESTAMP)) AS "ordering_1",
    DATE_TRUNC('MONTH', CAST("sbtransaction"."sbtxdatetime" AS TIMESTAMP)) AS "month"
  FROM "main"."sbtransaction" AS "sbtransaction"
  WHERE
    "sbtransaction"."sbtxdatetime" <= CAST('2023-03-31' AS DATE)
    AND "sbtransaction"."sbtxdatetime" >= CAST('2023-01-01' AS DATE)
    AND "sbtransaction"."sbtxstatus" = 'success'
  GROUP BY
    DATE_TRUNC('MONTH', CAST("sbtransaction"."sbtxdatetime" AS TIMESTAMP))
)
SELECT
  "_t0"."month" AS "month",
  "_t0"."avg_price" AS "avg_price"
FROM "_t0" AS "_t0"
ORDER BY
  "_t0"."ordering_1"
