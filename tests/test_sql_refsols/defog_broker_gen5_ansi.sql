SELECT
  DATE_TRUNC('MONTH', CAST("sbtransaction"."sbtxdatetime" AS TIMESTAMP)) AS "month",
  AVG("sbtransaction"."sbtxprice") AS "avg_price"
FROM "main"."sbtransaction" AS "sbtransaction"
WHERE
  "sbtransaction"."sbtxdatetime" <= CAST('2023-03-31' AS DATE)
  AND "sbtransaction"."sbtxdatetime" >= CAST('2023-01-01' AS DATE)
  AND "sbtransaction"."sbtxstatus" = 'success'
GROUP BY
  DATE_TRUNC('MONTH', CAST("sbtransaction"."sbtxdatetime" AS TIMESTAMP))
ORDER BY
  "month"
