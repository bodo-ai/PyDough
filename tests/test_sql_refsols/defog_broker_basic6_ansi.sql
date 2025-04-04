WITH _t0 AS (
  SELECT
    sbdpdate AS date,
    sbdptickerid AS ticker_id
  FROM main.sbdailyprice
  WHERE
    sbdpdate >= CAST('2023-04-01' AS DATE)
), _t1 AS (
  SELECT
    ticker_id AS ticker_id
  FROM _t0
), _t0_2 AS (
  SELECT
    sbtickerid AS _id
  FROM main.sbticker
)
SELECT
  _t0._id AS _id
FROM _t0_2 AS _t0
WHERE
  EXISTS(
    SELECT
      1 AS "1"
    FROM _t1
    WHERE
      _id = ticker_id
  )
