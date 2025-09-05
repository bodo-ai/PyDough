WITH _s0 AS (
  SELECT
    COUNT(sbdpclose) AS count_sbdpclose,
    MAX(sbdphigh) AS max_sbdphigh,
    MIN(sbdplow) AS min_sbdplow,
    CONCAT_WS(
      '-',
      YEAR(CAST(sbdpdate AS TIMESTAMP)),
      LPAD(MONTH(CAST(sbdpdate AS TIMESTAMP)), 2, '0')
    ) AS month,
    SUM(sbdpclose) AS sum_sbdpclose,
    sbdptickerid
  FROM main.sbdailyprice
  GROUP BY
    4,
    6
), _t0 AS (
  SELECT
    MAX(_s0.max_sbdphigh) AS max_sbdphigh,
    MIN(_s0.min_sbdplow) AS min_sbdplow,
    SUM(_s0.count_sbdpclose) AS sum_count_sbdpclose,
    SUM(_s0.sum_sbdpclose) AS sum_sum_sbdpclose,
    _s0.month,
    sbticker.sbtickersymbol
  FROM _s0 AS _s0
  JOIN main.sbticker AS sbticker
    ON _s0.sbdptickerid = sbticker.sbtickerid
  GROUP BY
    5,
    6
)
SELECT
  sbtickersymbol AS symbol,
  month,
  sum_sum_sbdpclose / sum_count_sbdpclose AS avg_close,
  max_sbdphigh AS max_high,
  min_sbdplow AS min_low,
  (
    (
      sum_sum_sbdpclose / sum_count_sbdpclose
    ) - LAG(sum_sum_sbdpclose / sum_count_sbdpclose, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month)
  ) / LAG(sum_sum_sbdpclose / sum_count_sbdpclose, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month) AS momc
FROM _t0
