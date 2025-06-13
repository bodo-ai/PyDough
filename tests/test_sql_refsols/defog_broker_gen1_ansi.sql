WITH _s0 AS (
  SELECT
    MIN(sbdpclose) AS lowest_price,
    sbdptickerid AS ticker_id
  FROM main.sbdailyprice
  WHERE
    DATEDIFF(CURRENT_TIMESTAMP(), sbdpdate, DAY) <= 7
  GROUP BY
    sbdptickerid
)
SELECT
  MIN(_s0.lowest_price) AS lowest_price
FROM _s0 AS _s0
JOIN main.sbticker AS sbticker
  ON _s0.ticker_id = sbticker.sbtickerid AND sbticker.sbtickersymbol = 'VTI'
