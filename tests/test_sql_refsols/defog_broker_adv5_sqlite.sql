WITH _s2 AS (
  SELECT
    SUM(sbdpclose) AS expr_0,
    COUNT(sbdpclose) AS expr_1,
    MAX(sbdphigh) AS max_high,
    MIN(sbdplow) AS min_low,
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbdpdate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbdpdate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbdpdate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbdpdate) AS INTEGER), (
          2 * -1
        ))
      END
    ) AS month,
    sbdptickerid AS ticker_id
  FROM main.sbdailyprice
  GROUP BY
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbdpdate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbdpdate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbdpdate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbdpdate) AS INTEGER), (
          2 * -1
        ))
      END
    ),
    sbdptickerid
), _t1 AS (
  SELECT
    SUM(_s2.expr_0) AS expr_0,
    SUM(_s2.expr_1) AS expr_1,
    MAX(_s2.max_high) AS max_high,
    MIN(_s2.min_low) AS min_low,
    _s2.month,
    _s1.sbtickersymbol AS symbol
  FROM _s2 AS _s2
  JOIN main.sbticker AS _s1
    ON _s1.sbtickerid = _s2.ticker_id
  GROUP BY
    _s2.month,
    _s1.sbtickersymbol
)
SELECT
  symbol,
  month,
  CAST(expr_0 AS REAL) / expr_1 AS avg_close,
  max_high,
  min_low,
  CAST((
    (
      CAST(expr_0 AS REAL) / expr_1
    ) - LAG((
      CAST(expr_0 AS REAL) / expr_1
    ), 1) OVER (PARTITION BY symbol ORDER BY month)
  ) AS REAL) / LAG((
    CAST(expr_0 AS REAL) / expr_1
  ), 1) OVER (PARTITION BY symbol ORDER BY month) AS momc
FROM _t1
