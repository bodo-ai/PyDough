WITH _s0 AS (
  SELECT
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbdpdate AS TIMESTAMP)),
      LPAD(CAST(EXTRACT(MONTH FROM CAST(sbdpdate AS TIMESTAMP)) AS TEXT), 2, '0')
    ) AS month,
    sbdptickerid,
    COUNT(sbdpclose) AS count_sbdpclose,
    MAX(sbdphigh) AS max_sbdphigh,
    MIN(sbdplow) AS min_sbdplow,
    SUM(sbdpclose) AS sum_sbdpclose
  FROM main.sbdailyprice
  GROUP BY
    1,
    2
), _t0 AS (
  SELECT
    _s0.month,
    sbticker.sbtickersymbol,
    MAX(_s0.max_sbdphigh) AS max_max_sbdphigh,
    MIN(_s0.min_sbdplow) AS min_min_sbdplow,
    SUM(_s0.count_sbdpclose) AS sum_count_sbdpclose,
    SUM(_s0.sum_sbdpclose) AS sum_sum_sbdpclose
  FROM _s0 AS _s0
  JOIN main.sbticker AS sbticker
    ON _s0.sbdptickerid = sbticker.sbtickerid
  GROUP BY
    1,
    2
)
SELECT
  sbtickersymbol AS symbol,
  month,
  CAST(sum_sum_sbdpclose AS DOUBLE PRECISION) / sum_count_sbdpclose AS avg_close,
  max_max_sbdphigh AS max_high,
  min_min_sbdplow AS min_low,
  CAST((
    (
      CAST(sum_sum_sbdpclose AS DOUBLE PRECISION) / sum_count_sbdpclose
    ) - LAG(CAST(sum_sum_sbdpclose AS DOUBLE PRECISION) / sum_count_sbdpclose, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month)
  ) AS DOUBLE PRECISION) / LAG(CAST(sum_sum_sbdpclose AS DOUBLE PRECISION) / sum_count_sbdpclose, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month) AS momc
FROM _t0
