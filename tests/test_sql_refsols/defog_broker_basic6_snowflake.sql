SELECT
  sbtickerid AS _id
FROM broker.sbticker
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM broker.sbdailyprice
    WHERE
      sbdpdate >= CAST('2023-04-01' AS DATE) AND sbdptickerid = sbticker.sbtickerid
  )
