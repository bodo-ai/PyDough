SELECT
  sbtickerid AS _id,
  sbtickersymbol AS symbol
FROM broker.sbTicker
WHERE
  NOT EXISTS(
    SELECT
      1 AS `1`
    FROM broker.sbDailyPrice
    WHERE
      sbdptickerid = sbTicker.sbtickerid
  )
