SELECT
  sbtickerid AS _id,
  sbtickersymbol AS symbol
FROM main.sbticker
WHERE
  NOT EXISTS(
    SELECT
      1 AS "1"
    FROM main.sbdailyprice
    WHERE
      sbdptickerid = sbticker.sbtickerid
  )
