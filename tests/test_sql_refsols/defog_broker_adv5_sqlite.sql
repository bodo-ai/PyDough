WITH _s1 AS (
  SELECT
    sbtickerid AS _id,
    sbtickersymbol AS symbol
  FROM main.sbticker
), _s4 AS (
  SELECT DISTINCT
    CONCAT_WS(
      '-',
      CAST(STRFTIME('%Y', sbdailyprice.sbdpdate) AS INTEGER),
      CASE
        WHEN LENGTH(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER)) >= 2
        THEN SUBSTRING(CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), 1, 2)
        ELSE SUBSTRING('00' || CAST(STRFTIME('%m', sbdailyprice.sbdpdate) AS INTEGER), -2)
      END
    ) AS month,
    _s1.symbol
  FROM main.sbdailyprice AS sbdailyprice
  JOIN _s1 AS _s1
    ON _s1._id = sbdailyprice.sbdptickerid
), _s2 AS (
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
), _t2 AS (
  SELECT
    SUM(_s2.expr_0) AS expr_0,
    SUM(_s2.expr_1) AS expr_1,
    MAX(_s2.max_high) AS max_high,
    MIN(_s2.min_low) AS min_low,
    _s2.month,
    _s3.symbol
  FROM _s2 AS _s2
  JOIN _s1 AS _s3
    ON _s2.ticker_id = _s3._id
  GROUP BY
    _s2.month,
    _s3.symbol
)
SELECT
  _s4.symbol,
  _s4.month,
  CAST(_t2.expr_0 AS REAL) / _t2.expr_1 AS avg_close,
  _t2.max_high,
  _t2.min_low,
  CAST((
    (
      CAST(_t2.expr_0 AS REAL) / _t2.expr_1
    ) - LAG((
      CAST(_t2.expr_0 AS REAL) / _t2.expr_1
    ), 1) OVER (PARTITION BY _s4.symbol ORDER BY _s4.month)
  ) AS REAL) / LAG((
    CAST(_t2.expr_0 AS REAL) / _t2.expr_1
  ), 1) OVER (PARTITION BY _s4.symbol ORDER BY _s4.month) AS momc
FROM _s4 AS _s4
JOIN _t2 AS _t2
  ON _s4.month = _t2.month AND _s4.symbol = _t2.symbol
