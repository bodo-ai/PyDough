WITH _s0 AS (
  SELECT
    COUNT(sbdpclose) AS count_sbdpclose,
    MAX(sbdphigh) AS max_high,
    MIN(sbdplow) AS min_low,
    SUM(sbdpclose) AS sum_sbdpclose,
    sbdptickerid
  FROM main.sbdailyprice
  GROUP BY
    CONCAT_WS(
      '-',
      YEAR(CAST(sbdpdate AS TIMESTAMP)),
      LPAD(MONTH(CAST(sbdpdate AS TIMESTAMP)), 2, '0')
    ),
    5
), _t0 AS (
  SELECT
    MAX(_s0.max_high) AS max_high,
    MIN(_s0.min_low) AS min_low,
    SUM(_s0.count_sbdpclose) AS sum_count_sbdpclose,
    SUM(_s0.sum_sbdpclose) AS sum_sum_sbdpclose
  FROM _s0 AS _s0
  JOIN main.sbticker AS sbticker
    ON _s0.sbdptickerid = sbticker.sbtickerid
  GROUP BY
    sbticker.sbtickersymbol,
    CONCAT_WS('-', YEAR(CAST(date AS TIMESTAMP)), LPAD(MONTH(CAST(date AS TIMESTAMP)), 2, '0'))
)
SELECT
  symbol_1 AS symbol,
  CONCAT_WS('-', YEAR(CAST(date AS TIMESTAMP)), LPAD(MONTH(CAST(date AS TIMESTAMP)), 2, '0')) AS month,
  sum_sum_sbdpclose / sum_count_sbdpclose AS avg_close,
  max_high,
  min_low,
  (
    (
      sum_sum_sbdpclose / sum_count_sbdpclose
    ) - LAG(sum_sum_sbdpclose / sum_count_sbdpclose, 1) OVER (PARTITION BY symbol_1 ORDER BY CONCAT_WS('-', YEAR(CAST(date AS TIMESTAMP)), LPAD(MONTH(CAST(date AS TIMESTAMP)), 2, '0')))
  ) / LAG(sum_sum_sbdpclose / sum_count_sbdpclose, 1) OVER (PARTITION BY symbol_1 ORDER BY CONCAT_WS('-', YEAR(CAST(date AS TIMESTAMP)), LPAD(MONTH(CAST(date AS TIMESTAMP)), 2, '0'))) AS momc
FROM _t0
