SELECT
  sbtickerid AS _id
FROM broker.sbTicker
WHERE
  EXISTS(
    SELECT
      1 AS `1`
    FROM broker.sbDailyPrice
    WHERE
      sbdpdate >= CAST('2023-04-01' AS DATE) AND sbdptickerid = sbTicker.sbtickerid
  )
