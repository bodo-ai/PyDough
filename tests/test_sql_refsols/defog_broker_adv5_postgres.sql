WITH _s0 AS (
  SELECT
    COUNT(sbdpclose) AS count_sbdpclose,
    MAX(sbdphigh) AS max_high,
    MIN(sbdplow) AS min_low,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbdpdate AS TIMESTAMP)),
      LPAD(EXTRACT(MONTH FROM CAST(sbdpdate AS TIMESTAMP)), 2, '0')
    ) AS month,
    SUM(sbdpclose) AS sum_sbdpclose,
    sbdptickerid
  FROM main.sbdailyprice
  GROUP BY
    4,
    6
), _t0 AS (
  SELECT
    MAX(_s0.max_high) AS max_high,
    MIN(_s0.min_low) AS min_low,
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
  CAST(sum_sum_sbdpclose AS DOUBLE PRECISION) / sum_count_sbdpclose AS avg_close,
  max_high,
  min_low,
  CAST((
    (
      CAST(sum_sum_sbdpclose AS DOUBLE PRECISION) / sum_count_sbdpclose
    ) - LAG(CAST(sum_sum_sbdpclose AS DOUBLE PRECISION) / sum_count_sbdpclose, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month)
  ) AS DOUBLE PRECISION) / LAG(CAST(sum_sum_sbdpclose AS DOUBLE PRECISION) / sum_count_sbdpclose, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month) AS momc
FROM _t0
