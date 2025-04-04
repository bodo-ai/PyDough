SELECT
  DATE("sbtransaction"."sbtxdatetime", 'start of month') AS "month",
  AVG("sbtransaction"."sbtxprice") AS "avg_price"
FROM "main"."sbtransaction" AS "sbtransaction"
WHERE
  "sbtransaction"."sbtxdatetime" <= '2023-03-31'
  AND "sbtransaction"."sbtxdatetime" >= '2023-01-01'
  AND "sbtransaction"."sbtxstatus" = 'success'
GROUP BY
  DATE("sbtransaction"."sbtxdatetime", 'start of month')
ORDER BY
  "month"
