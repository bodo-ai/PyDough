WITH _t1 AS (
  SELECT
    MAX(sbdphigh) AS agg_0,
    MIN(sbdplow) AS agg_1,
    sbdptickerid AS ticker_id
  FROM main.sbdailyprice
  WHERE
    sbdpdate <= CAST('2023-04-04' AS DATE) AND sbdpdate >= CAST('2023-04-01' AS DATE)
  GROUP BY
    sbdptickerid
), _t0_2 AS (
  SELECT
    _t1.agg_0 - _t1.agg_1 AS ordering_2,
    _t1.agg_0 - _t1.agg_1 AS price_change,
    sbticker.sbtickersymbol AS symbol
  FROM main.sbticker AS sbticker
  LEFT JOIN _t1 AS _t1
    ON _t1.ticker_id = sbticker.sbtickerid
  ORDER BY
    ordering_2 DESC
  LIMIT 3
)
SELECT
  symbol,
  price_change
FROM _t0_2
ORDER BY
  ordering_2 DESC
