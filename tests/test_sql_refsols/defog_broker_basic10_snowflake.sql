SELECT
  sbtickerid AS _id,
  sbtickersymbol AS symbol
FROM broker.sbticker
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM broker.sbdailyprice
    WHERE
      sbdptickerid = sbticker.sbtickerid
  )
