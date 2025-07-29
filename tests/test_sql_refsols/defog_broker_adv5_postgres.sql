WITH _s0 AS (
  SELECT
    COUNT(sbdpclose) AS count_sbdpclose,
    MAX(sbdphigh) AS max_high,
    MIN(sbdplow) AS min_low,
    SUM(sbdpclose) AS sum_sbdpclose,
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbdpdate AS TIMESTAMP)),
      LPAD(EXTRACT(MONTH FROM CAST(sbdpdate AS TIMESTAMP)), 2, '0')
    ) AS month,
    sbdptickerid
  FROM main.sbdailyprice
  GROUP BY
    CONCAT_WS(
      '-',
      EXTRACT(YEAR FROM CAST(sbdpdate AS TIMESTAMP)),
      LPAD(EXTRACT(MONTH FROM CAST(sbdpdate AS TIMESTAMP)), 2, '0')
    ),
    sbdptickerid
), _t0 AS (
  SELECT
    CAST(SUM(_s0.sum_sbdpclose) AS DOUBLE PRECISION) / SUM(_s0.count_sbdpclose) AS avg_close,
    MAX(_s0.max_high) AS max_high,
    MIN(_s0.min_low) AS min_low,
    _s0.month,
    sbticker.sbtickersymbol
  FROM _s0 AS _s0
  JOIN main.sbticker AS sbticker
    ON _s0.sbdptickerid = sbticker.sbtickerid
  GROUP BY
    _s0.month,
    sbticker.sbtickersymbol
)
SELECT
  sbtickersymbol AS symbol,
  month,
  avg_close,
  max_high,
  min_low,
  CAST((
    avg_close - LAG(avg_close, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month)
  ) AS DOUBLE PRECISION) / LAG(avg_close, 1) OVER (PARTITION BY sbtickersymbol ORDER BY month) AS momc
FROM _t0
