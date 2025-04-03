SELECT
  "sbtransaction"."sbtxid" AS "transaction_id",
  CAST(STRFTIME('%H', "sbtransaction"."sbtxdatetime") AS INTEGER) AS "_expr0",
  CAST(STRFTIME('%M', "sbtransaction"."sbtxdatetime") AS INTEGER) AS "_expr1",
  CAST(STRFTIME('%S', "sbtransaction"."sbtxdatetime") AS INTEGER) AS "_expr2"
FROM "main"."sbtransaction" AS "sbtransaction"
LEFT JOIN "main"."sbticker" AS "sbticker"
  ON "sbticker"."sbtickerid" = "sbtransaction"."sbtxtickerid"
WHERE
  "sbticker"."sbtickersymbol" IN ('AAPL', 'GOOGL', 'NFLX')
ORDER BY
  "transaction_id"
